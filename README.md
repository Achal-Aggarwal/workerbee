# workerbee
A worker bee for your hive.

    // Creating a Database and printing its create ddl statement
    TestNew testNewDB = new TestNew();
    Query databaseCreator = create(testNewDB).ifNotExist();
    System.out.println(databaseCreator.generate());
    //-- CREATE DATABASE IF NOT EXISTS TestNew COMMENT 'TestNewComments'
    //-- LOCATION 'Path' WITH DBPROPERTIES ( 'key2' = 'value2', 'key1' = 'value1' ) ;

    // Creating a Table and printing its create ddl statement
    User userTable = new User(testNewDB);
    System.out.println(create(userTable).ifNotExist().generate());
    //-- CREATE TABLE IF NOT EXISTS TestNew.User ( id INT, name STRING, lastName STRING )
    //-- COMMENT 'User table' ;

    UserContact userContactTable = new UserContact(testNewDB);
    System.out.println(create(userContactTable).ifNotExist().generate());
    //-- CREATE TABLE IF NOT EXISTS TestNew.UserContact ( id INT, userId INT,
    //-- contactNumber STRING ) COMMENT 'User contacts table' ;

    // Generating select query to select all columns from table user
    System.out.println(select(star()).from(userTable).generate());
    //-- SELECT * FROM TestNew.User ;

    // Generating select query to select first four digits of contact number
    Query firstFourDigits =
      select(substr(userContactTable.contactNumber, 1, 4).as("firstFour"))
        .from(userContactTable);
    System.out.println(firstFourDigits.generate());
    //-- SELECT SUBSTR(UserContact.contactNumber, 1, 4) AS firstFour FROM TestNew.UserContact ;

    // Making a join between two tables
    SelectQuery userNameAndContactJoin =
      select(userTable.name, userContactTable.contactNumber)
        .from(userTable)
        .join(userContactTable).on(userTable.id, userContactTable.userId)
        .as("UserNameWithContacts");
    System.out.println(userNameAndContactJoin.generate());
    //-- SELECT User.name, UserContact.contactNumber FROM TestNew.User JOIN TestNew.UserContact
    //-- ON User.id = UserContact.userId AS UserNameWithContacts ;

    // Creating schema for the joined table
    Table flatUserContactTable = userNameAndContactJoin.table();
    System.out.println(create(flatUserContactTable).generate());
    //-- CREATE TABLE UserNameWithContacts ( name STRING, contactNumber STRING ) ;


    String ramSharmaRecord = "1" + userTable.getColumnSeparator() + "Ram"
      + userTable.getColumnSeparator()
      + "Sharma";
    System.out.println(ramSharmaRecord);
    //-- 1RamSharma

    // Parsing a record (deserializing) to generate a row of corresponding table.
    Row user = userTable.parseRecordUsing(ramSharmaRecord);

    String name = (String) user.get(userTable.name);
    //-- 1
    Integer id = (Integer) user.get(userTable.id);
    //-- Ram
    String lastName = (String) user.get(userTable.lastName);
    //-- Sharma

    System.out.println(id);
    System.out.println(name);
    System.out.println(lastName);

    // Modifying the row object and generating record (serializing)
    user
      .set(userTable.name, "Mohan")
      .set(userTable.id, "2");
    String record = userTable.generateRecordFor(user);
    System.out.println(record);
    //-- 2MohanSharma
