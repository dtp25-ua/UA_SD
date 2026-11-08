import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.BufferedReader;

// clase solo para mostrar por pantalla el estado de las WS
public class PanelCentral {

    // colores de fondo (ANSI), se desactivan definiendo la variable NO_COLOR
    private static final boolean COLOR = System.getenv("NO_COLOR") == null;
    private static final String RESET = "\u001B[0m";
	private static final String VERDE = "\u001B[42;30m";
	private static final String VERDE_INTENSO = "\u001B[102;30m";   
	private static final String ROJO = "\u001B[41;97m";
	private static final String NARANJA = "\u001B[48;5;208;30m";
	private static final String GRIS = "\u001B[100;97m";

    // definimos el formato de fecha y hora
	private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    // para simular el parpadeo de las WS en riego
    public static boolean fase = false;

    // pinta el texto con el color indicado si no se ha desactivado la opción de color
    private static String pintar(String p_texto, String p_color)
    {
        if (COLOR)
            return p_color + p_texto + RESET;
        else
            return p_texto;
    }

    private static String colorDe(String p_estado)
    {
        switch (p_estado) 
        {
            case EstacionInfo.DISPONIBLE:     return VERDE;
            case EstacionInfo.REGANDO:        return fase ? VERDE_INTENSO : VERDE;
            case EstacionInfo.FUGA:           return ROJO;
            case EstacionInfo.FUERA_SERVICIO: return NARANJA;
            default:                          return GRIS;

        }
    }

    // devuelve el texto de estado a mostrar en el panel central
	private static String etiquetaDe(String p_estado)
	{
		switch (p_estado)
		{
			case EstacionInfo.FUERA_SERVICIO: return "FUERA DE SERVICIO";
			case EstacionInfo.REGANDO:        return fase ? "REGANDO *" : "REGANDO";
			default:                          return p_estado;
		}
	}

    // muestra la info de las estaciones en el panel principal de Central
    public static void imprimir(GestorEstaciones p_gestor, String p_info)
    {
        fase = !fase;   // alternamos la fase para simular el parpadeo de las WS en riego

        // ordenamos las estaciones por id
        List<EstacionInfo> lista = new ArrayList<EstacionInfo>(p_gestor.getAllEstaciones());
        lista.sort(Comparator.comparing(EstacionInfo::getIdEstacion));

        int disponibles = 0, regando = 0, fugas = 0, fuera = 0, desconectadas = 0;
        for (EstacionInfo e : lista)
		{
			switch (e.getEstadoVisible())
			{
				case EstacionInfo.DISPONIBLE:     disponibles++;  break;
				case EstacionInfo.REGANDO:        regando++;      break;
				case EstacionInfo.FUGA:           fugas++;        break;
				case EstacionInfo.FUERA_SERVICIO: fuera++;        break;
				default:                          desconectadas++;
			}
		}

        System.out.println("--- WM_Central | " + p_info + " | " + LocalTime.now().format(HORA) + " ---");
		System.out.println("Estaciones: " + lista.size() + " | Disponibles: " + disponibles + " | Regando: " + regando
			+ " | Fuga: " + fugas + " | Fuera de servicio: " + fuera + " | Desconectadas: " + desconectadas);
		System.out.println();

        // formato para tabla
		System.out.println(String.format("%-8s %-18s %-19s %12s %11s  %s",
			"ID", "UBICACION", "ESTADO", "CAUDAL L/MIN", "VOLUMEN L", "OPERARIO"));
        
        for (EstacionInfo e : lista)
        {
            String estado = e.getEstadoVisible();
            boolean regandoAhora = estado.equals(EstacionInfo.REGANDO);
			boolean conectada = !estado.equals(EstacionInfo.DESCONECTADA);

            String caudal = "-";
			String volumen = "-";
			String operario = "-";

            if (regandoAhora)
			{
                // locale.us (regional usa) para que el decimal sea siempre . y no ,
				caudal = String.format(Locale.US, "%.2f", e.getCaudalActual());
				volumen = String.format(Locale.US, "%.2f", e.getVolumenAcumulado());
				if (e.getIdOperarioRiego() != null)
					operario = e.getIdOperarioRiego();
			}
			else if (conectada)
			{
				caudal = "0.00";
				volumen = "0.00";
			}

            String celdaEstado = String.format("%-19s", etiquetaDe(estado));
			System.out.println(String.format("%-8s %-18s ", e.getIdEstacion(), e.getUbicacion())
				+ pintar(celdaEstado, colorDe(estado))
				+ String.format(" %12s %11s  %s", caudal, volumen, operario));
        }
    }

    // implementa el panel en tiempo real 
    public static void enVivo(GestorEstaciones p_gestor, String p_info, BufferedReader p_br)
    {
        // atomicBoolean para que lo puedan modificar varios hilos a la vez
        AtomicBoolean salir = new AtomicBoolean(false);

        // hilo nuevo que comprueba si se pulsa enter por consola, para no bloquear el resto del metodo
        Thread lector = new Thread(() -> {
            try { p_br.readLine(); }
            catch (IOException e) { }
            salir.set(true);
        });
        lector.setDaemon(true);  // para que el hilo termine cuando termine el main
		lector.start();

        try
        {
            // bucle hasta que el usuario pida salir (salir = false)
            while (!salir.get())
            {
                if (COLOR)
                    System.out.print("\u001B[2J\u001B[H");   // borra la pantalla y vuelve arriba
				imprimir(p_gestor, p_info);     // imprimimos el panel
                System.out.println("\n(pulsa Enter para volver al menu)");

                // esperamos 1s comprobando si se pulsó enter para salir
                for (int i = 0; i < 10 && !salir.get(); i++)
					Thread.sleep(100);
            }
        }
        catch (InterruptedException e)
        {
            // nadie interrumpe este hilo
        }
    }
}