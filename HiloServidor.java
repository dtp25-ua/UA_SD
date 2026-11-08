import java.lang.Exception;
import java.net.Socket;
import java.net.SocketTimeoutException;
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

	// metodo auxiliar para obtener el nombre de la estacion asociada a este hilo
	private String nombre()
	{
		// if - else
		return (idEstacion != null) ? idEstacion : "cliente sin registrar";
	}

	// comprueba si la estacion asociada a este hilo es la misma que la pasada como parametro
	private boolean estacionValida(String p_id)
	{
		return idEstacion != null && idEstacion.equals(p_id);
	}

	// metodo para cambiar el estado de la estacion en BD y en memoria
	private String cambiarEstado(String p_id, String p_nuevoEstado)
	{
		if (!estacionValida(p_id))
		{
			return "STATUS#ERROR#Estacion no registrada en esta conexion";
		}

		try
		{
			// actualizamos el estado en la BD
			gestorBD.updateEstado(p_id, p_nuevoEstado);

			// actualizamos el estado en memoria, para que el panel de control lo muestre
			EstacionInfo est =gestorEstaciones.getEstacion(p_id);
			if (est != null)
			{
				est.setEstado(p_nuevoEstado);
			}
			System.out.println("SRV: Estacion " + p_id + " cambio de estado a " + p_nuevoEstado);
			return "STATUS#OK#Estado actualizado correctamente";
		}
		catch (SQLException e)
		{
			System.out.println("Error en la BD al cambiar estado: " + e.toString());
			return "STATUS#ERROR#No se pudo actualizar el estado de la estacion";
		}
	}

	// decide que hacer segun el tipo de trama recibida  y devuelve la respuesta
	private String procesarTrama(String p_Cadena)
	{
		// separamos por # la trama
		String[] campos = p_Cadena.split("#");
		String tipo = campos[0];
		
		if(campos.length == 3 && tipo.equals("REGISTRO"))
		{
			// registramos la estacion en la BD y en memoria
			return procesarRegistro(campos[1], campos[2]);
		} 
		else if (campos.length == 3 && tipo.equals("HEARTBEAT"))
		{
			// el latido no cambia el estado, sirve como aviso de que la estacion sigue viva
			if (estacionValida(campos[1]))
			{
				return "STATUS#OK#Latido recibido";
			}
			else 
			{
				return "STATUS#ERROR#Estacion no registrada en esta conexion";
			}
		}
		else if (campos.length == 3 && tipo.equals("AVERIA"))
		{
			// por implementar cuando haya riegos: si estaba regando, finalizarlo
			return cambiarEstado(campos[1], EstacionInfo.FUGA);
		}
		else if (campos.length == 2 && tipo.equals("AVERIA_RESUELTA"))
		{
			return cambiarEstado(campos[1], EstacionInfo.DISPONIBLE);
		}
		return "STATUS#ERROR#Trama no válida";
	}

	// metodo para registrar una estacion en la BD y en memoria
	private String procesarRegistro(String p_id, String p_ubicacion)
	{
		try
		{
			// registramos la estacion en la BD
			gestorBD.upsertEstacion(p_id, p_ubicacion, EstacionInfo.DISPONIBLE);

			// actualizamos tambien el estado en memoria, para que el panel de control lo muestre
			EstacionInfo est = gestorEstaciones.registrarOActualizar(p_id, p_ubicacion, EstacionInfo.DISPONIBLE);

			// asociamos la conexion a la estacion
			est.setConexionActual(this);
			
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
	
	// se ejecuta siempre cuando termina la conexion
	private void MarcarDesconectada()
	{
		if (idEstacion != null)
		{
			EstacionInfo estacion = gestorEstaciones.getEstacion(idEstacion);
			if (estacion != null)
			{ 
				// comprobamos tambien que es la conexion actual de la estacion 
				if (estacion.getConexionActual() != this)
					return;
				
				// actualizamos el estado en memoria 
				estacion.setEstado(EstacionInfo.DESCONECTADA);
			}
			try
			{
				// actualizamos el estado en la BD
				gestorBD.updateEstado(idEstacion, EstacionInfo.DESCONECTADA);				
			}
			catch (SQLException e)
			{
				System.out.println("Error en la BD al marcar estacion como desconectada: " + e.toString());
			}

			// si no ha habido ningun error
			System.out.println("SRV: Estacion " + idEstacion + " desconectada");
		}
	}

	// metodo principal del hilo, se ejecuta al iniciar el hilo
    public void run() 
	{
        try 
		{	
			skCliente.setSoTimeout(timeout);
			entrada = new DataInputStream(skCliente.getInputStream());
			salida = new DataOutputStream(skCliente.getOutputStream());
			
			// bucle infinito para atender al cliente mientras no haya error de conexion
			for(;;)
			{
				// por cada trama recibida del cliente enviamos una respuesta
				String trama = leeSocket();
				String respuesta = procesarTrama(trama);
				escribeSocket(respuesta);
			}
        }
        catch (SocketTimeoutException e) {
          System.out.println("SRV: sin mensajes de vida de " + nombre());
        }
		catch (EOFException e) {
          System.out.println("SRV: " + nombre() + " ha cerrado la conexion");
        }
		catch (IOException e) {
          System.out.println("SRV: error de comunicación con " + nombre() + ": " + e.toString());
        }
		catch (Exception e) {
          System.out.println("Error: " + e.toString());
        }
		finally 
		{
			MarcarDesconectada();
			try{
				skCliente.close();
			}
			catch (IOException e)
			{
				System.out.println("Error al cerrar el socket: " + e.toString());
			}
		}
    }
}
