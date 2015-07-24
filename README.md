### Workerbee
It’s a bee that could be used to perform various task with Apache Hive. Inspired from tools like Beetest, HiveRunner, hive_test.

### Schema at one place
Workerbee enables you to define schema of database and tables along side of Map - Reduce programs. Once written they can be managed by changing java code. Using MigrationGenerator, migrations can generated which can be run against the database that need to migrate.

### Query Builder at disposal
Workerbee comes with basic query builder that comes handy when you don’t want to commit any typos while writing queries. No need to remember or lookup tables and their schema just to check column's order and type.

### Go with TDD
TDD is a wonderful approach to write minimal code required to do the work at hand. Workerbee allows you to write tests using unit test frameworks like JUnit. Using it you can formulate test for each case scenario and run it against setup data. Using data objects, created for each row of concerning table, more explicit assertions can made to get better assertion message on test failure.

### Domain model object to use in Map-Reduce programs
When requirements gets complex and hive query could not handle it, writing map reduce program is a good option. But to read data and construct objects on which operations could be performed, knowledge of raw format or schema of the table is needed. Workerbee take care of this requirement, by extending Row of a table its behaviour could be extended.

### Supported Hive version
Currently workerbee supports hive version 0.13, but query builder doesn’t support everything that is in it, yet.

### Example scenario
With baseball statistics like bats man name, runs scored, the year for years 1871 - 2011, find the player with the highest runs for each year.

**Setting up project:**

I. Clone the repo and build with mvn install

II. Add dependency 
```xml
<dependency>
   <groupId>com.workerbee</groupId>
   <artifactId>workerbee-core</artifactId>
   <version>1.0-SNAPSHOT</version>
</dependency>
```
III. Add env variable HADOOP_HOME which points to your hadoop location.

IV. Make sure hive-exec*.jar is present at HADOOP_HOME/share/hadoop/common/lib

**Creating Database & table:**
```java
public class BaseBall extends Database {
  public static final BaseBall db = new BaseBall();

  private BaseBall() {
    super("BaseBall", "BaseBall database");
  }
}

public class Batting extends Table<Batting> {
  public static final Batting tb = new Batting();

  public static final Column playerId = HavingColumn(tb, "player_id", Column.Type.STRING);
  public static final Column year     = HavingColumn(tb, "year",      Column.Type.INT);
  public static final Column runs     = HavingColumn(tb, "runs",      Column.Type.INT);

  private Batting() {
    super(BaseBall.db, "Batting", "Batting table", 1);
  }
}
```

**Writing Test - Using Junit4:**
```java
public class PlayerWithHighestRunForEachYear() {
  public static final String PLAYER_1_ID = "PLAYER1_ID";
  public static final String PLAYER_2_ID = "PLAYER2_ID";
  public static final String PLAYER_3_ID = "PLAYER3_ID";
  private Repository repo;
  private Row<Batting> lowestRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_1_ID)
    .set(Batting.year, 1990)
    .set(Batting.runs, 10);

  private Row<Batting> mediumRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_2_ID)
    .set(Batting.year, 1990)
    .set(Batting.runs, 100);

  private Row<Batting> maximumRun
    = Batting.tb.getNewRow()
    .set(Batting.playerId, PLAYER_3_ID)
    .set(Batting.year, 2000)
    .set(Batting.runs, 50);

  public BaseBallTest() throws IOException, SQLException {
    repo = Repository.TemporaryRepository();
    repo.execute(create(BaseBall.db).ifNotExist());
  }

  @Test
  public void shouldReturnPlayerWithHighestScoreForEachYear() throws IOException, SQLException {
    repo.setUp(Batting.tb);
    repo.setUp(Batting.tb)
      .setUp(Batting.tb, lowestRun, mediumRun, maximumRun);

    List<Row<Table>> years = repo.execute(QUERY_TO_TEST);

    assertThat(years.size(), is(2));

    assertThat(years.get(0).getString(Batting.playerId), is(PLAYER_2_ID));
    assertThat(years.get(0).getInt(Batting.year), is(1990));
    assertThat(years.get(0).getInt(Batting.runs), is(100));

    assertThat(years.get(1).getString(Batting.playerId), is(PLAYER_3_ID));
    assertThat(years.get(1).getInt(Batting.year), is(2000));
    assertThat(years.get(1).getInt(Batting.runs), is(50));
  }
}
```
**Writing queries**
The next step is to group the data by year so we can find the highest score for each year. This query first groups all the records by year and then selects the player with the highest runs from each year.
```java
select(Batting.year, max(Batting.runs))
  .from(Batting.tb)
  .groupBy(Batting.year)
```
Now we need to go back and get the player_id(s) so we know who the player(s) was. We know that for a given year we can use the runs to find the player(s) for that year. So we can take the previous query and join it with the batting records to get the final table.
```java
SelectQuery selectQuery = select(Batting.year, max(Batting.runs))
  .from(Batting.tb)
  .groupBy(Batting.year)
  .ascOrderOf(Batting.year)
  .as("MaxRunsForEachYear");

Table<Table> maxRunsForEachYear = selectQuery.table();

select(Batting.playerId, Batting.year, Batting.runs).from(Batting.tb)
  .join(selectQuery)
  .on(Batting.year.eq(maxRunsForEachYear.getColumn(Batting.year))
      .and(Batting.runs.eq(maxRunsForEachYear.getColumn(Batting.runs)))
  )
```
