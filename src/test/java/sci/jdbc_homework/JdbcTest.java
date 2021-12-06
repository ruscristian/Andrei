package sci.jdbc_homework;

import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.h2.tools.RunScript;
import org.h2.tools.Server;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.util.Scanner;


public class JdbcTest {

    Connection connection;


    @Before
    public void setUp() {
        try {
            Class.forName("org.h2.Driver").newInstance();
            connection = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE", "sa", "");
            Statement statement = connection.createStatement();

//            RunScript.execute(connection, new FileReader("src/main/resources/deleteAllTables.sql"));
//            RunScript.execute(connection, new FileReader("src/main/resources/deleteAllData.sql"));
            String createTable =
                    "CREATE TABLE accommodation\n" +
                            "(\n" +
                            "id serial NOT NULL,\n" +
                            "type character varying(32) NOT NULL,\n" +
                            "bed_type character varying(32) NOT NULL,\n" +
                            "max_guests integer NOT NULL,\n" +
                            "description character varying(512),\n" +
                            "PRIMARY KEY (id)\n" +
                            ");\n" +
                            "\n" +
                            "CREATE TABLE room_fare\n" +
                            "(\n" +
                            "id serial NOT NULL,\n" +
                            "value double precision NOT NULL,\n" +
                            "season character varying(32),\n" +
                            "PRIMARY KEY (id)\n" +
                            ");\n" +
                            "\n" +
                            "CREATE TABLE accommodation_fare_relation\n" +
                            "(\n" +
                            "id serial PRIMARY KEY NOT NULL,\n" +
                            "id_accommodation integer NOT NULL,\n" +
                            "FOREIGN KEY (id_accommodation)\n" +
                            "REFERENCES accommodation (id)\n" +
                            "id_room_fare integer NOT NULL,\n" +
                            "FOREIGN KEY (id_room_fare)\n" +
                            "REFERENCES room_fare (id));";
            String dropTable = "drop table accommodation;\n" +
                    "drop table room_fare;\n" +
                    "drop table accommodation_fare_relation;";


//            int create = statement.executeUpdate(createTable);
            int drop = statement.executeUpdate(dropTable);


//try {
//    RunScript.execute(connection, new FileReader("src/main/resources/createTables.sql"));
//} catch (JdbcSQLSyntaxErrorException e){
//    RunScript.execute(connection, new FileReader("src/main/resources/deleteAllTables.sql"));
//}finally {
//    RunScript.execute(connection, new FileReader("src/main/resources/createTables.sql"));
//}

            populateTable("src/main/resources/accommodationTable.csv", connection,
                          "accommodation",
                          "(type, bed_type, max_guests, description)",
                          "(?, ?, ?, ?)");

            populateTable("src/main/resources/roomFareTable.csv", connection,
                          "room_fare",
                          "(value, season)",
                          "(?, ?)");


            connection.commit();
            connection.close();


        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void populateTable(String fileDataForPopulating, Connection conn,
                              String tableName, String tableColumnsName, String columnsNumber) throws SQLException, FileNotFoundException {


        Scanner dataFromFile = new Scanner(new BufferedReader(new FileReader(fileDataForPopulating)));
        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + tableColumnsName + "VALUES " + columnsNumber);

        dataFromFile.nextLine();
        dataFromFile.useDelimiter(",");
        String input = dataFromFile.nextLine();
        if (tableName.equals("accommodation")) {
            while (dataFromFile.hasNextLine()) {
                getDataFromAccommodationCsvLine(ps, input);
            }
        }
        else if (tableName.equals("room_fare")) {
            while (dataFromFile.hasNextLine()) {
                getDataFromRoomFareCsvLine(ps, input);
            }
        }
        else {
            System.out.println("Not such table found");
        }

    }

    private void insertRow(PreparedStatement ps, String type, String bed_type, int max_guests, String description) throws SQLException {
        ps.setString(1, type);
        ps.setString(2, bed_type);
        ps.setInt(3, max_guests);
        ps.setString(4, description);
        ps.executeUpdate();
    }

    public void getDataFromAccommodationCsvLine(PreparedStatement ps, String input) throws SQLException {
        String[] data = input.split(",");

        if (data.length != 4) {
            throw new ArrayIndexOutOfBoundsException();
        }

        String type = data[0].trim();
        String bed_type = data[1].trim();
        Integer max_guest = Integer.valueOf(data[2].trim());
        String description = data[3].trim();

        insertRow(ps, type, bed_type, max_guest, description);

    }

    private void insertRow(PreparedStatement ps, int value, String season) throws SQLException {

        ps.setInt(2, value);
        ps.setString(3, season);
        ps.executeUpdate();
    }

    public void getDataFromRoomFareCsvLine(PreparedStatement ps, String input) throws SQLException {
        String[] data = input.split(",");

        if (data.length != 2) {
            throw new ArrayIndexOutOfBoundsException();
        }

        Integer value = Integer.valueOf(data[0].trim());
        String season = data[1].trim();

        insertRow(ps, value, season);
    }

//    private void insertDataInFareRelation(PreparedStatement ps, int id) throws SQLException
//    {
//        ps.setInt(1, id);
//        ps.setInt(2, value);
//        ps.setString(3, season);
//        ps.executeUpdate();
//
//        //TODO query for inserting data to this table
//    }


    public String selectAllFromTable(String tableName) {
        return "SELECT * FROM " + tableName;
    }


    @Test
    public void testSelect() throws SQLException
    {
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM accommodation");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int guests = resultSet.getInt("max_guests");
            String type = resultSet.getString("type");
            System.out.println(type+ " : " + guests);
        }
    }


}