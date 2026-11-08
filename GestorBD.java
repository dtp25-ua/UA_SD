import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

// gestor de acceso a la base de datos SQLite
public class GestorBD {
    private String rutaBD;
    private Connection conexion;

    public GestorBD(String rutaBD) {
        this.rutaBD = rutaBD;
    }

    // abre la conexion a la bd y crea las tablas si no existen
    public synchronized void inicializar() throws SQLException {
        conexion = DriverManager.getConnection("jdbc:sqlite:" + rutaBD);
        crearTablas();
    }

    private void crearTablas() throws SQLException {
        String sqlEstaciones = 
            "CREATE TABLE IF NOT EXISTS estaciones (" +
            " id_estacion TEXT PRIMARY KEY," +
            " ubicacion  TEXT NOT NULL," +
            " estado     TEXT NOT NULL DEFAULT 'DESCONECTADA'," +
            " fecha_alta TEXT DEFAULT CURRENT_TIMESTAMP" +
            ");";

        String sqlOperarios = 
            "CREATE TABLE IF NOT EXISTS operarios (" +
            " id_operario TEXT PRIMARY KEY," +
            " nombre      TEXT" +
            ");";

        String sqlRiegos = 
            "CREATE TABLE IF NOT EXISTS riegos (" +
            " id_riego        INTEGER PRIMARY KEY AUTOINCREMENT," +
            " id_estacion     TEXT NOT NULL," +
            " id_operario     TEXT," +
            " inicio          TEXT DEFAULT CURRENT_TIMESTAMP," +
            " fin             TEXT," +
            " volumen_total_l REAL," +
            " duracion_s      INTEGER," +
            " motivo_fin      TEXT," +
            " FOREIGN KEY (id_estacion) REFERENCES estaciones(id_estacion)," +
            " FOREIGN KEY (id_operario) REFERENCES operarios(id_operario)" +
            ");";

        try (Statement stmt = conexion.createStatement()) {
            stmt.execute(sqlEstaciones);
            stmt.execute(sqlOperarios);
            stmt.execute(sqlRiegos);
        }
    }

    // inserta una estación si no existe, o actualiza ubicacion/estado si ya existe
    public synchronized void upsertEstacion(String p_id, String p_ubicacion, String p_estado) throws SQLException
    {
        String sql = "INSERT INTO estaciones (id_estacion, ubicacion, estado) VALUES (?, ?, ?) " +
                     "ON CONFLICT(id_estacion) DO UPDATE SET ubicacion = excluded.ubicacion, estado = excluded.estado;";

        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, p_id);
            ps.setString(2, p_ubicacion);
            ps.setString(3, p_estado);
            ps.executeUpdate();
        }
    }

    // actualiza el estado de una estación con un id dado
    public synchronized void updateEstado(String p_id, String p_estado) throws SQLException
    {
        String sql = "UPDATE estaciones SET estado = ? WHERE id_estacion = ?;";

        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, p_estado);
            ps.setString(2, p_id);
            ps.executeUpdate();
        }
    }

    // carga todas las estaciones de la BD y devuelve una lista de estaciones
    public synchronized List<EstacionInfo> cargarEstaciones() throws SQLException
    {
        List<EstacionInfo> lista = new ArrayList<EstacionInfo>();
        String sql = "SELECT id_estacion, ubicacion, estado FROM estaciones";

        // ejecutar el select
        try (Statement stmt = conexion.createStatement();
            ResultSet rs = stmt.executeQuery(sql))
        {
            // leemos mientras haya filas por leer
            while (rs.next())
            {
                lista.add(new EstacionInfo(
                    rs.getString("id_estacion"),
                    rs.getString("ubicacion"),
                    EstacionInfo.DESCONECTADA));  // al arrancar Central siempre mostramos la WS como desconectada
            }
        }
        return lista;
    }

    // registra el resumen de un riego ya terminado en la BD
    public synchronized void insertarRiego(String p_idEstacion, String p_idOperario,  double p_volumenTotal, int p_duracion, String p_motivoFin) throws SQLException
    {
        String sql = "INSERT INTO riegos (id_estacion, id_operario, fin, volumen_total_l, duracion_s, motivo_fin)" + 
        "VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?);";

        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, p_idEstacion);
            ps.setString(2, p_idOperario);
            ps.setDouble(3, p_volumenTotal);
            ps.setInt(4, p_duracion);
            ps.setString(5, p_motivoFin);
            ps.executeUpdate();
        }
    }

    // metodo para cerrar la conexion a la BD
    public synchronized void cerrar()
    {
        try {
            if (conexion != null && !conexion.isClosed()) {
                conexion.close();
            }
        } catch (SQLException e) {
            System.out.println("Error al cerrar la BD: " + e.toString());
        }
    }
}
