package sci.jdbc_homework;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;


public class JdbcTest {

    Connection connection;
    final static String createTable =
            "DROP TABLE IF EXISTS accommodation cascade;\n" +
                    "DROP TABLE IF EXISTS room_fare cascade;\n" +
                    "DROP TABLE IF EXISTS accommodation_fare_relation cascade;\n" +

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
                    "value integer NOT NULL,\n" +
                    "season character varying(32),\n" +
                    "PRIMARY KEY (id)\n" +
                    ");\n" +
                    "\n";

    final static String createRelationTable =
            "CREATE TABLE accommodation_fare_relation\n" +
                    "(\n" +
                    "id serial PRIMARY KEY NOT NULL,\n" +
                    "id_accommodation integer NOT NULL,\n" +
                    "FOREIGN KEY (id_accommodation)\n" +
                    "REFERENCES accommodation(id),\n" +
                    "id_room_fare integer NOT NULL,\n" +
                    "FOREIGN KEY (id_room_fare)\n" +
                    "REFERENCES room_fare(id));";

    @Before
    public void setUp() throws IOException {
        try {
            Class.forName("org.h2.Driver").newInstance();

            System.out.println("Connecting to a selected database...");
            connection = DriverManager.getConnection("jdbc:h2:~/test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE", "sa", "");
            System.out.println("Successfully connected to database ...");

            Statement statement = connection.createStatement();

            System.out.println(".....................................................");
            System.out.println("Creating tables in database...");

            statement.executeUpdate(createTable);
            statement.executeUpdate(createRelationTable);

            System.out.println("Creating tables in database successful...");


            System.out.println(".....................................................");
            System.out.println("Populating tables...");

            populateTable("src/main/resources/accommodationTable.csv", connection,
                    "accommodation",
                    "(type, bed_type, max_guests, description)",
                    "(?, ?, ?, ?)");

            System.out.println("Successful Accommodation table...");

            populateTable("src/main/resources/roomFareTable.csv", connection,
                    "room_fare",
                    "(value, season)",
                    "(?, ?)");

            System.out.println("Successful Room Fare table...");

            populateTable("^", connection,
                    "accommodation_fare_relation",
                    "(id_accommodation,id_room_fare)",
                    "(?, ?)");

            System.out.println("Successful Relation table...");

            System.out.println(".....................................................");
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void populateTable(String fileDataForPopulating, Connection conn,
                              String tableName, String tableColumnsName, String columnsNumber) throws SQLException, FileNotFoundException {

        Scanner dataFromFile = null;
        try {
            dataFromFile = new Scanner(new BufferedReader(new FileReader(fileDataForPopulating)));
        } catch (FileNotFoundException e) {
            System.out.println(String.format("File \"%s\" not found. Probably because it doesn't need to be found.", fileDataForPopulating));
//            e.printStackTrace();
        }
        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + tableColumnsName + "VALUES " + columnsNumber);

        String input = "";
        if (dataFromFile != null) {
            dataFromFile.nextLine();
            dataFromFile.useDelimiter(",");
            input = dataFromFile.nextLine();
        }

        if (tableName.equals("accommodation")) {
            while (dataFromFile.hasNextLine()) {
                getDataFromAccommodationCsvLine(ps, input);
                input = dataFromFile.nextLine();
            }
        } else if (tableName.equals("room_fare")) {
            while (dataFromFile.hasNextLine()) {
                getDataFromRoomFareCsvLine(ps, input);
                input = dataFromFile.nextLine();
            }

        } else if (tableName.equals("accommodation_fare_relation")) {
            PreparedStatement accommodation = conn.prepareStatement(getSelectAllQuery("accommodation"));
            ResultSet accResultSet = accommodation.executeQuery();
            PreparedStatement fareData = conn.prepareStatement(getSelectAllQuery("room_fare"));
            ResultSet fareResultSet = fareData.executeQuery();


            ArrayList<Integer> accIdList = new ArrayList<>();
            while (accResultSet.next()) {
                accIdList.add(accResultSet.getInt("id"));
            }

            ArrayList<Integer> fareIdList = new ArrayList<>();
            while (fareResultSet.next()) {
                fareIdList.add(fareResultSet.getInt("id"));
            }

            int i = 0;
            for (Integer id : fareIdList) {
                ps.setInt(1, accIdList.get(i % (Math.min(fareIdList.size(), accIdList.size()))));
                ps.setInt(2, id);
                ps.executeUpdate();
                i++;
            }
        } else {
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
        ps.setInt(1, value);
        ps.setString(2, season);
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


    public String getSelectAllQuery(String tableName) {
        return "SELECT * FROM " + tableName;
    }


    @Test
    public void testSelect2() throws SQLException {
        PreparedStatement ps = connection.prepareStatement(getSelectAllQuery("accommodation"));
        ResultSet resultSet = ps.executeQuery();
        String format = "%5s%15s%30s%10s%37s\n";
        System.out.format(format, "-ID-", "-TYPE-", "-BED-","-GUEST-", "-DESCRIPTION-");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String type = resultSet.getString("type");
            String bed = resultSet.getString("bed_type");
            int guestNr = resultSet.getInt("max_guests");
            String description = resultSet.getString("description");
            System.out.format(format, id, type, bed, guestNr, description);
        }
        System.out.println("\n");
        PreparedStatement ps2 = connection.prepareStatement(getSelectAllQuery("room_fare"));
        ResultSet resultSet2 = ps2.executeQuery();
        format = "%5s%15s%5s\n";
        System.out.format(format, "-ID-", "-SEASON", "-VALUE-");
        while (resultSet2.next()) {
            int id = resultSet2.getInt("id");
            int value = resultSet2.getInt("value");
            String season = resultSet2.getString("season");
            System.out.format(format, id, season, value);
        }
        System.out.println("\n");
        format = "%15s%15s\n";
        PreparedStatement ps1 = connection.prepareStatement(getSelectAllQuery("accommodation_fare_relation"));
        ResultSet resultSet1 = ps1.executeQuery();
        System.out.format(format, "Accommodation ID", "Room Fare ID");
        while (resultSet1.next()) {
            int id = resultSet1.getInt("id_accommodation");
            int id2 = resultSet1.getInt("id_room_fare");

            System.out.format(format, id, id2);
        }
    }

    @Test
    public void testJoin() throws SQLException {

        ResultSet result = connection.createStatement().executeQuery(
                "Select a.type, b.value, b.season " +
                        "from accommodation a " +
                        "inner join accommodation_fare_relation ac " +
                        "on  a.id = ac.id_accommodation " +
                        "inner join room_fare b " +
                        "on b.id = ac.id_room_fare " +
                        "order by b.value, b.season");

        String format = "%15s%10s%20s\n";
        if (result.next()) {
            System.out.format(format, "-TYPE-", "-PRICE-", "-SEASON-");
            while (result.next()) {
                System.out.format(format,
                        result.getString("type"),
                        result.getInt("value"),
                        result.getString("season"));
            }

        } else {
            System.out.println("Nada resultados");
        }
    }


}