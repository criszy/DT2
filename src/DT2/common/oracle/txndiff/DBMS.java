package DT2.common.oracle.txndiff;

public enum DBMS {
    MYSQL("mysql"), MARIADB("mysql"), TIDB("mysql"),
    POSTGRESQL("postgresql"), COCKROACHDB("postgresql"), ZNBASE("postgresql");

    private final String protocol;

    DBMS(String protocol) {
        this.protocol = protocol;
    }

}
