package DT2.arangodb;

@FunctionalInterface
public interface ArangoDBQueryProvider<S> {
    ArangoDBQueryAdapter getQuery(S globalState) throws Exception;
}
