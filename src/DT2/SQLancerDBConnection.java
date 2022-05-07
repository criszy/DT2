package DT2;

public interface SQLancerDBConnection extends AutoCloseable {

    String getDatabaseVersion() throws Exception;
}
