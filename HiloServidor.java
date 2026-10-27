import java.lang.Exception;
import java.net.Socket;
import java.sql.SQLException;
import java.io.*;

public class HiloServidor extends Thread {

	private Socket skCliente;
	private GestorBD gestorBD;
	private GestorEstaciones gestorEstaciones;

	// tiempo maximo de espera para recibir datos del cliente, en milisegundos
	private int timeout;

	// flujos de datos que se abren una vez y se usan durante toda la conexion
	private DataInputStream entrada;
	private DataOutputStream salida;

	// estacion asociada a esta conexion, vacia hasta que se registre
	private String idEstacion = null;
	
	public HiloServidor(Socket p_cliente, GestorBD p_gestorBD, GestorEstaciones p_gestorEstaciones, int p_timeout)
	{
		this.skCliente = p_cliente;
		this.gestorBD = p_gestorBD;
		this.gestorEstaciones = p_gestorEstaciones;
		this.timeout = p_timeout;
	}
	
	// lee una trama del socket cliente, si hay error lanza la excepcion a run()
	private String leeSocket () throws IOException
	{
		return entrada.readUTF();
	}

	// escribe una trama en el socket
	private void escribeSocket (String p_Datos) throws IOException
	{
		salida.writeUTF(p_Datos);

		// vaciamos el buffer 
		salida.flush();
	}

	private String nombre()
	{
		// if - else
		return (idEstacion != null) ? "HiloServidor" : "cliente sin registrar";
	}

	// comprueba si la estacion asociada a este hilo es la misma que la pasada como parametro
	private boolean estacionValida(String p_id)
	{
		return idEstacion != null && idEstacion.equals(p_id);
	}

	private String procesarTrama(String p_Cadena)
	{
		// separamos por # la trama
		String[] campos = p_Cadena.split("#");
		String tipo = campos[0];
		
		// comprobamos que el formato es el adecuado
		if(campos.length == 3 && tipo.equals("REGISTRO"))
		{
			return procesarRegistro(campos[1], campos[2]);

			try
			{
				gestorBD.upsertEstacion((idEstacion), ubicacion, EstacionInfo.DISPONIBLE);

				// actualizamos tambien el estado en memoria, para que el panel de control lo muestre
				gestorEstaciones.registrarOActualizar(idEstacion, ubicacion, EstacionInfo.DISPONIBLE);

				respuesta = "STATUS#OK#Estacion registrada correctamente";
			}
			catch (SQLException e){

				// si falla la insercion a la BD se lo comunicamos al cliente como error
				System.out.println("Error en la BD al registrar estacion: " + e.toString());
				respuesta = "STATUS#ERROR#No se pudo registrar la estacion";
			}
		} 
		else if (campos.length == 3 && tipo.equals("HEARTBEAT"))
		{
			if (!estacionValida(campos[1]))
			{
				return "STATUS#OK#Latido recibido";
			}
			else 
			{
				return "STATUS#ERROR#Estacion no registrada en esta conexion";
			}
		}
		else if (campos.length == 3 && tipo.equals("HEARTBEAT"))
	}

	// procesa la trama de registro recibida del cliente
	private String procesarRegistro(String p_id, String p_ubicacion)
	{
		try
		{
			gestorBD.upsertEstacion((p_id), p_ubicacion, EstacionInfo.DISPONIBLE);

			// actualizamos tambien el estado en memoria, para que el panel de control lo muestre
			gestorEstaciones.registrarOActualizar(p_id, p_ubicacion, EstacionInfo.DISPONIBLE);
			idEstacion = p_id; // guardamos la estacion asociada a este hilo para poder actualizar su estado cuando se desconecte
			System.out.println("Estacion " + p_id + " registrada desde: " + p_ubicacion);

			return "STATUS#OK#Estacion registrada correctamente";
		}
		catch (SQLException e){

			// si falla la insercion a la BD se lo comunicamos al cliente como error
			System.out.println("Error en la BD al registrar estacion: " + e.toString());
			return "STATUS#ERROR#No se pudo registrar la estacion";
		}
	}
	
	
    public void run() {
		String Cadena="";
		
        try {
			
			Cadena = this.leeSocket (skCliente, Cadena);
			/*
			* Se escribe en pantalla la informacion que se ha recibido del
			* cliente
			*/
			String respuesta = this.procesarRegistro(Cadena);
			this.escribeSocket (skCliente, respuesta);						
			
			skCliente.close();
			//System.exit(0); No se debe poner esta sentencia, porque en ese caso el primer cliente que cierra rompe el socket 
			//				  y desconecta a todos				
        }
        catch (Exception e) {
          System.out.println("Error: " + e.toString());
        }
    }
}
