import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class MenuCentral implements Runnable {

    private GestorEstaciones gestor;
    private String info;
    private BufferedReader br;

    public MenuCentral(GestorEstaciones p_gestor, String p_info)
	{
		this.gestor = p_gestor;
		this.info = p_info;
		this.br = new BufferedReader(new InputStreamReader(System.in));
	} 

    // punto unico por el que salen las ordenes hacia las WS.
	// por implementar: sustituir por un productor Kafka cuando se integre.
	private void publicarOrden(String p_trama)
	{
		System.out.println("[Kafka pendiente] " + p_trama);
	}

    // para determinar las estaciones a regar
    private List<EstacionInfo> elegirDestino() throws IOException 
    {
        // inicializamos la lista vacia
        List<EstacionInfo> destino = new ArrayList<EstacionInfo>();

        System.out.println("ID de la Ws (o 'TODAS'): ");
        String id = br.readLine();
        if (id == null)
            return destino;
        // trim para eliminar espacios
        id = id.trim();

        if (id.equalsIgnoreCase("TODAS"))
        {   
            destino.addAll(gestor.getAllEstaciones());
            if (destino.isEmpty())
                System.out.println("No hay estaciones registradas en la BD");
        }
        else
        {
            // recorremos todas las estaciones buscando la del id pedido
            for (EstacionInfo e : gestor.getAllEstaciones())
            {
                if (e.getIdEstacion().equalsIgnoreCase(id))
					destino.add(e);
            }
            if (destino.isEmpty())
				System.out.println("No existe ninguna WS con ID " + id);
        }
        return destino;
    }

    //
    private void iniciarRiego() throws IOException 
    {
        List<EstacionInfo> destino = elegirDestino();
		if (destino.isEmpty())
			return;

        System.out.print("Duracion maxima del riego (s): ");
		String texto = br.readLine();
		if (texto == null)
			return;

        int duracion;
        try
        {
            duracion = Integer.parseInt(texto.trim());
        }
        catch (NumberFormatException e)
        {
            System.out.println("La duracion debe ser mayor que 0");
			return;
        }

        // recorremos la lista de estaciones que tienen el id dado
        for (EstacionInfo e : destino)
		{
			// Central valida que la WS este disponible antes de dar la orden
			if (!e.estaDisponible())
			{
				System.out.println(e.getIdEstacion() + ": no disponible (" + e.getEstadoVisible()
					+ "), riego no iniciado");
				continue;  // para pasar a la siguiente estacion sin terminar el bucle
			}
            // a falta de kafka
			publicarOrden("ORDEN_REGAR#" + e.getIdEstacion() + "#CENTRAL#" + duracion);
			System.out.println(e.getIdEstacion() + ": orden de riego enviada");
		}
    }

    // para bloquear / activar una estacion
    private void cambiarBloqueo(boolean p_bloquear) throws IOException
	{
		for (EstacionInfo e : elegirDestino())
		{
			e.setBloqueada(p_bloquear);
			publicarOrden((p_bloquear ? "ORDEN_BLOQUEAR#" : "ORDEN_ACTIVAR#") + e.getIdEstacion());
			System.out.println(e.getIdEstacion() + (p_bloquear ? ": fuera de servicio" : ": activada"));
		}
	}

    public void run()
    {
        try
        {
            for(;;)
            {
                System.out.println("\n--- Menu de WM_Central ---");
				System.out.println("[1] Panel en vivo");
				System.out.println("[2] Iniciar riego (una WS o todas)");
				System.out.println("[3] Bloquear WS (una o todas)");
				System.out.println("[4] Activar WS (una o todas)");

                String linea = br.readLine();
				if (linea == null)
				{
					// sin teclado (por ejemplo un contenedor): el menu se apaga, Central sigue en marcha
					System.out.println("Sin entrada de teclado: menu desactivado");
					return;
				}

                switch (linea.trim())
				{
					case "1": PanelCentral.enVivo(gestor, info, br); break;
					case "2": iniciarRiego();        break;
					case "3": cambiarBloqueo(true);  break;
					case "4": cambiarBloqueo(false); break;
					default:  System.out.println("Opcion no valida");
				}
            }
        }
        catch (IOException e)
        {
            System.out.println("Error en la entrada de datos: " + e.toString());
        }
    }


}
