import java.io.*;
import java.net.*;

// clase que implementa el cliente que se conecta al servidor WM_Central para registrar una estacion
public class WM_WS_M {

	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
	public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = flujo.readUTF();
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	public void escribeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			OutputStream aux = p_sk.getOutputStream();
			DataOutputStream flujo= new DataOutputStream( aux );
			flujo.writeUTF(p_Datos);      
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return;
	}
	
	// nuevo metodo que sustituye a pedirOperacion	
	public void registrarEstacion(String p_idEstacion, String p_ubicacion, Socket p_Socket_Con_Servidor)
	{
		// construimos la trama de registro y la enviamos al servidor
		String trama = "REGISTRO#" + p_idEstacion + "#" + p_ubicacion;
		escribeSocket(p_Socket_Con_Servidor, trama);

		// leemos la respuesta del servidor y la mostramos por pantalla
		String respuesta = "";
		respuesta = leeSocket(p_Socket_Con_Servidor, respuesta);
		System.out.println("Respuesta del servidor: " + respuesta);

		// cerramos el socket con el servidor
		try {
			p_Socket_Con_Servidor.close();
		} catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}
	
	// metodo modificado para recibir el id y la ubicacion de la estacion
	public void menu(String p_host, String p_puerto, String p_idEstacion, String p_ubicacion)
	{
		int opc = 0;
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader (isr);
		try
		{
			
			while (opc!=1 && opc!=2)
			{
				System.out.println("[1] Registrar estacion\n");
				System.out.println("[2] Salir");
				System.out.println("Indique la opcion a realizar:");
				
				opc = Integer.parseInt(br.readLine());
			}
			if (opc == 1)
			{
				Socket skCliente = new Socket(p_host, Integer.parseInt(p_puerto));
				registrarEstacion(p_idEstacion, p_ubicacion, skCliente);
			}
			else
			{
				System.exit(0);
			}
		}catch(Exception e)
		{
			System.out.println("Error " + e.toString());
		}
		return;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		WM_WS_M cl = new WM_WS_M();
		int i = 0;
		String host;
		String puerto;
		String idEstacion;
		String ubicacion;
		if (args.length < 4) {
			// pedimos también el id y la ubicacion (anexo)
			System.out.println ("Debe indicar la direccion del servidor, el puerto, el ID de la estacion y la ubicacion");
			System.out.println ("$./WM_WS_M nombre_servidor puerto_servidor id_estacion ubicacion");
			System.exit(-1);
		}
		host = args[0];
		puerto = args[1];
		idEstacion = args[2];
		ubicacion = args[3];

		while(i==0)
		{
			cl.menu(host, puerto, idEstacion, ubicacion);
		}
	}

}
