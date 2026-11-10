import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

// clase para manejar toda la comunicacion con Kafka
public class MensajeriaKafka {

    // direccion del servidor Kafka
    private String bootstrap;
	private KafkaProducer<String, String> productor;

	public MensajeriaKafka(String p_host, String p_puerto)
	{
		this.bootstrap = p_host + ":" + p_puerto;
	}

    public void iniciarProductor()
	{
		Properties p = new Properties();    // para guardar clave-valor
		p.put("bootstrap.servers", bootstrap);
		p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("acks", "all");            // el broker confirma cuando el mensaje esta guardado
		p.put("max.block.ms", "5000");   // sin broker, falla a los 5 s en vez de esperar un minuto
		productor = new KafkaProducer<String, String>(p);
	}

    // publica y espera la confirmacion del broker, devuelve true si se ha entregado.
	public boolean publicar(String p_topic, String p_clave, String p_valor)
	{
		try
		{
            // crea y envia el mensaje, y espera el resultado 10s
			productor.send(new ProducerRecord<String, String>(p_topic, p_clave, p_valor)).get(10, TimeUnit.SECONDS);
			return true;
		}
		catch (Exception e)
		{
			System.out.println("[KAFKA] No se pudo publicar en " + p_topic + ": " + e.toString());
			return false;
		}
	}

    // lanza un hilo que lee el topic y llama al manejador (clave, valor) por cada mensaje
	public void suscribir(String p_topic, String p_grupo, String p_offsetInicial,
	                      boolean p_confirmarOffsets, BiConsumer<String, String> p_manejador)
    {
        Thread hilo = new Thread(() -> {
			Properties p = new Properties();
			p.put("bootstrap.servers", bootstrap);
			p.put("group.id", p_grupo);     // grupo de consumidores
			p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			p.put("auto.offset.reset", p_offsetInicial);
			p.put("enable.auto.commit", String.valueOf(p_confirmarOffsets));

			KafkaConsumer<String, String> consumidor = null;
			try
			{
				consumidor = new KafkaConsumer<String, String>(p);

				// lo pasamos a lista Collections, necesario para subscribe()
				consumidor.subscribe(Collections.singletonList(p_topic));

				// bucle infinito para recibir mensajes que llegan al topic y procesarlos
				for(;;)
				{
					// consultar mensajes en kafka (clave-valor), maximo 0.5s de espera
					ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(500));
					for (ConsumerRecord<String, String> r : registros)	// recorremos los mensajes recibidos
					{
						try
						{
							p_manejador.accept(r.key(), r.value());
						}
						catch (Exception e)
						{
							// un mensaje raro no debe tumbar el consumidor
							System.out.println("[KAFKA] Error al tratar un mensaje de " + p_topic + ": " + e.toString());
						}
					}
				}
			}
			catch (Exception e)
			{
				System.out.println("[KAFKA] Consumidor de " + p_topic + " detenido: " + e.toString());
			}
			finally
			{
				if (consumidor != null)
					consumidor.close();
			}
        });
		hilo.setDaemon(true);
		hilo.start();
    }
}
