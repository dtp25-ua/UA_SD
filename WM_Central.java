import java.net.*;
import java.sql.SQLException;
import java.util.List;

public class WM_Central {

	/**
	 * @param args puerto_servidor kafka_host kafka_puerto ruta_bd timeout_ms
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		/*
		* Descriptores de socket servidor y de socket con el cliente
		*/
		String puerto="";
		String kafkaHost="";
		String kafkaPuerto="";

		try
		{
			if (args.length < 5) {
				System.out.println("Debe indicar el puerto, IP y puerto de Kafka, ruta de la BD y timeout(ms)");
				System.out.println("$./WM_Central puerto_servidor kafka_host kafka_puerto ruta_bd timeout_ms");
				System.exit (1);
			}

			// guardamos los argumentos de entrada por linea de comandos
			puerto = args[0];
			kafkaHost = args[1];
			kafkaPuerto = args[2];
			String rutaBD = args[3];
			int timeout = Integer.parseInt(args[4]);
			
			// inicializamos la base de datos
			GestorBD gestorBD = new GestorBD(rutaBD);
			gestorBD.inicializar();

			// estado en memoria de todas las estaciones compartido entre hilos
			GestorEstaciones gestorEstaciones = new GestorEstaciones();

			// cargamos las estaciones ya conocidas y las registramos como desconectadas al arrancar
			List<EstacionInfo> estacionesPrevias = gestorBD.cargarEstaciones();
			for (EstacionInfo e : estacionesPrevias)
			{
				gestorEstaciones.registrarOActualizar(e.getIdEstacion(), e.getUbicacion(), EstacionInfo.DESCONECTADA);
			}
			System.out.println("Cargadas " + estacionesPrevias.size() + " estaciones desde BD (estado inicial: DESCONECTADA)");

			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
		    System.out.println("Escucho el puerto " + puerto);
			System.out.println("Kafka configurado en " + kafkaHost + ":" + kafkaPuerto);

			/*
			* Mantenemos la comunicacion con el cliente
			*/	
			for(;;)
			{
				/*
				* Se espera un cliente que quiera conectarse
				*/
				Socket skCliente = skServidor.accept(); // Crea objeto
		        System.out.println("Sirviendo cliente...");

				// pasamos gestorBD y gestorEstaciones al hilo para que pueda consultar/actualizar el estado
		        Thread t = new HiloServidor(skCliente, gestorBD, gestorEstaciones, timeout);
		        t.start();
			}
		}
		// capturamos posibles excepciones de SQL
		catch(SQLException e)
		{
			System.out.println("Error de base de datos: " + e.toString());
		}
		// excepciones de otros tipos
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
			
		}
	}

}
