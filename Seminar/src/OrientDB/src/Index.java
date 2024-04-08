import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Random;

public class Index {
    public static String randomPhoneNumber() {
        String[] prefixes = {"09", "08", "07", "03", "02"};
        int prefixIndex = (int) (Math.random() * prefixes.length);
        String randomNumber = "";
        for (int i = 0; i < 7; i++) {
            randomNumber += (int) (Math.random() * 10);
        }
        return prefixes[prefixIndex] + randomNumber;
    }

    public static String randomAddress() {
        String[] countries = {"United States", "United Kingdom", "Canada", "France", "Australia"};
        int countryIndex = (int) (Math.random() * countries.length);
        String houseNumber = "";
        for (int i = 0; i < 3; i++) {
            houseNumber += (int) (Math.random() * 10);
        }
        String streetName = "";
        String[] streetNames = {"Main Street", "Elm Street", "Oak Street", "Maple Street", "Pine Street"};
        streetName = streetNames[(int) (Math.random() * streetNames.length)];
        String cityName = "";
        String[] cityNames = {"New York", "London", "Toronto", "Paris", "Sydney"};
        cityName = cityNames[(int) (Math.random() * cityNames.length)];
        String zipCode = "";
        for (int i = 0; i < 5; i++) {
            zipCode += (int) (Math.random() * 10);
        }
        return countries[countryIndex] + ", " + houseNumber + " " + streetName + ", " + cityName + " " + zipCode;
    }


    public static void main(String[] args) {
        OrientDB orientDB = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        ODatabaseSession db = orientDB.open("demodb", "root", "123");


        db.getMetadata().getSchema().dropClass("Customer");
        OClass cus = db.createVertexClass("Customer");
        String[] name = {"Aaron", "Mabel", "Lang", "Macayla", "Mackenzie", "Makenna", "Madison", "Mae", "Maeve", "Adam", "Adrian", "Aiden", "Alexander", "Alex", "Alfred", "Allan", "Allen", "Alvin", "Andrew", "Andy", "Angel", "Anthony", "Antonio", "Archer", "Arnold", "Arthur", "Ashley", "Ashton", "Austin", "Bailey", "Baker", "Baldwin", "Benjamin", "Ben", "Bentley", "Bernard", "Billy", "Blake", "Brandon", "Brendan", "Brian", "Brody", "Brooks", "Brown", "Bryan", "Bryce", "Bryson", "Buddy", "Burton", "Caleb", "Calvin", "Cameron", "Campbell", "Carl", "Carlos", "Carter", "Casey", "Chad", "Chandler", "Charles", "Charlie", "Chase", "Christian", "Christopher", "Clark", "Clayton", "Clinton", "Cody", "Cole", "Colin", "Connor", "Conrad", "Cooper", "Corey", "Craig", "Curtis", "Daniel", "Darren", "David", "Davis", "Dawson", "Dean", "Dennis", "Derek", "Devin", "Diego", "Dillon", "Dominic", "Donald", "Douglas", "Drew", "Duncan", "Dylan", "Earl", "Edward", "Edwin", "Elijah", "Elliot", "Elliott", "Ellis", "Elon", "Emerson", "Eric", "Erik", "Ethan", "Evan", "Everett", "Fabian", "Felix", "Fernando", "Fletcher", "Ford", "Forrest", "Foster", "Francis", "Frank", "Franklin", "Frederick", "Gabriel", "Garrett", "Gary", "Gavin", "George", "Gerald", "Gilbert", "Gordon", "Grant", "Grayson", "Gregory", "Griffin", "Hadley", "Hall", "Hamilton", "Hank", "Hannah", "Hanson", "Hardin", "Harley", "Harold", "Harper", "Harrison", "Harry", "Harvey", "Hayden", "Hayes", "Heath", "Hector", "Henry", "Herbert", "Herman", "Herschel", "Hiram", "Holden", "Holland", "Homer", "Horace", "Howard", "Hudson", "Hugh", "Hunter", "Ian", "Ibrahim", "Isaac", "Isaiah", "Israel", "Ivan", "Jack", "Jackson", "Jacob", "James", "Jamie", "Jason", "Jasper", "Javier", "Jay", "Jayden", "Jeff", "Jefferson", "Jeffrey", "Jeremiah", "Jeremy", "Jesse", "Jesus", "Jim", "Jimmy", "Joe", "Joel", "John", "Johnny", "Johnson", "Jonathan", "Jordan", "Joseph", "Joshua", "Josiah", "Julian", "Julius", "Justin", "Kane", "Karl", "Keith", "Kelly", "Kevin", "Kenneth", "Kent", "Kyle", "Lance", "Landon", "Larry", "Lars", "Lauren", "Lawrence", "Lawson", "Layton", "Lee", "Leonard", "Leroy", "Leslie", "Lester", "Levi", "Lewis", "Lincoln", "Logan", "Lorenzo", "Louis", "Luke", "Luther", "Lyle", "Mack", "Madden", "Madison", "Malcolm", "Manuel", "Marcus", "Mark", "Marlin", "Marshall", "Martin", "Mason", "Matthew", "Maurice", "Max", "Maxwell", "Michael", "Miles", "Miller", "Mitchell", "Mohammed", "Monroe", "Montgomery", "Morgan", "Morris", "Moses", "Murphy", "Murray", "Nathan", "Nathaniel", "Neil", "Nelson", "Nicholas", "Nick", "Noah", "Nolan", "Norman", "Norris", "Oliver", "Omar", "Owen", "Parker", "Patrick", "Paul", "Payton", "Pedro", "Perry", "Peter", "Peyton", "Philip", "Phillip", "Pierce", "Porter", "Powell", "Preston", "Price", "Prince", "Quentin", "Quinn", "Rafael", "Ralph", "Randall", "Randy", "Raphael", "Raymond", "Reed", "Reese", "Regan", "Reginald", "Reid", "Reilly", "Richard", "Richie", "Rick", "Ricky", "Robert", "Roberto", "Robin", "Robinson", "Rocco", "Rodney", "Roger", "Ronald", "Ronnie", "Ross", "Roy", "Ruben", "Russell", "Ryan", "Samuel", "Sanders", "Santiago", "Saul", "Sawyer", "Scott", "Sean", "Sebastian", "Seth", "Shane", "Shawn", "Sheldon", "Shelton", "Shepherd", "Sherman", "Sidney", "Silas", "Simon", "Simpson", "Sinclair", "Skyler", "Slade", "Slater", "Smith", "Solomon", "Spencer", "Stacy", "Stanley", "Stefan", "Stephen", "Sterling", "Steven", "Stewart", "Stone", "Stuart", "Sullivan", "Sydney", "Tanner", "Taylor", "Ted", "Terence", "Terry", "Theodore", "Thomas", "Thompson", "Tony", "Trevor", "Tristan", "Troy", "Tucker", "Turner", "Tyler", "Vance", "Vaughn", "Vincent", "Victor", "Wade", "Walker", "Wallace", "Walter", "Warren", "Wayne", "Webb", "Webster", "Wesley", "West", "Weston", "Wheeler", "William", "Willis", "Wilson", "Winston", "Wyatt", "Xavier", "Zachary", "Zane", "Zachariah", "Zane", "Abigail", "Addison", "Adriana", "Adrienne", "Agatha", "Agnes", "Aisha", "Alana", "Alba", "Alena", "Alexandra", "Alexis", "Alice", "Alicia", "Alison", "Alyssa", "Amanda", "Amelia", "Amy", "Andrea", "Angelina", "Anna", "Annabel", "Annalisa", "Anne", "Annette", "Annie", "April", "Ashley", "Ashton", "Aubrey", "Audrey", "Augusta", "Aurora", "Ava", "Avery", "Bailey", "Baker", "Baldwin", "Barbara", "Barbie", "Bernadette", "Bianca", "Billie", "Blair", "Blake", "Blakely", "Bonnie", "Brandy", "Brenda", "Breanna", "Bridget", "Brielle", "Brittney", "Brooke", "Brooklyn", "Brown", "Bryanna", "Bryce", "Brynn", "Brynlee", "Buckley", "Buffy", "Bunny", "Caitlin", "Callie", "Cameron", "Campbell", "Candace", "Candice", "Candy", "Cara", "Carla", "Carmen", "Caroline", "Carolyn", "Carrie", "Carson", "Carter", "Casey", "Cassandra", "Cassidy", "Catherine", "Cathy", "Catrina", "Cecilia", "Celeste", "Celia", "Celine", "Chandra", "Chanel", "Chantelle", "Charlene", "Charli", "Charlotte", "Chloe", "Christina", "Christine", "Ciara", "Cindy", "Claire", "Clara", "Clarissa", "Claudia", "Colleen", "Connie", "Cora", "Corey", "Courtney", "Crista", "Crystal", "Daisy", "Dakota", "Dana", "Danielle", "Daniela", "Daphne", "Darlene", "Darla", "Dawn", "Dayna", "Dea", "Dean", "Debra", "Deborah", "Dee", "Dee", "Dee", "Deena", "Delaney", "Delilah", "Della", "Delores", "Demi", "Denise", "Denisha", "Destiny", "Devin", "Devon", "Diamond", "Diana", "Diane", "Dina", "Dolores", "Dominique", "Donna", "Dora", "Dorothy", "Doris", "Dottie", "Dreama", "Drea", "Drew", "Ebony", "Eden", "Edith", "Edna", "Edward", "Elaine", "Eleanor", "Eliza", "Elizabeth", "Ella", "Ellen", "Ellie", "Elliott", "Ellis", "Elsa", "Emily", "Emma", "Erin", "Erica", "Erika", "Erin", "Esmeralda", "Esther", "Ethel", "Eugenia", "Evelyn", "Eve", "Faith", "Fallon", "Fannie", "Fanny", "Faye", "Felicia", "Felicity", "Fiona", "Florence", "Frances", "Francesca", "Francis", "Frankie", "Freda", "Frederica", "Frieda", "Fuchsia", "Gabriella", "Gabrielle", "Gail", "Gale", "Gayle", "Genesis", "Geneva", "Genevieve", "Georgette", "Georgia", "Georgina", "Geraldine", "Geri", "Gertrude", "Gianna", "Gigi", "Gina", "Ginger", "Ginny", "Gladys", "Gloria", "Grace", "Gracie", "Graciela", "Graham", "Grant", "Gray", "Grayson", "Green", "Greer", "Gretchen", "Greta", "Griffin", "Guadalupe", "Hadley", "Hailey", "Haley", "Hall", "Hallie", "Hannah", "Hanna", "Hanson", "Hardin", "Harley", "Harper", "Harriet", "Harrison", "Harry", "Harvey", "Hayden", "Hayes", "Hazel", "Heather", "Heaven", "Heidi", "Helen", "Helena", "Helene", "Henrietta", "Henry", "Hermosa", "Hester", "Hope", "Houston", "Howard", "Hudson", "Hunter", "Ida", "Imani", "India", "Indira", "Ines", "Ingrid", "Irene", "Iris", "Isabella", "Isabelle", "Isadora", "Isis", "Ivy", "Jackie", "Jacqueline", "Jade", "Jaiden", "Jaime", "Jami", "Jamie", "Jamila", "Jasmine", "Jan", "Jane", "Janet", "Janice", "Janie", "Janna", "Jannie", "January", "Jaqueline", "Jayla", "Jazmine", "Jean", "Jeanne", "Jeannette", "Jeannie", "Jenna", "Jennifer", "Jenny", "Jessica", "Jill", "Jillian", "Jilly", "Joan", "Joanna", "Joanne", "Jocelyn", "Jodie", "Jody", "Johanna", "Jolene", "Jolie", "Jonelle", "Jordan", "Josefa", "Josephine", "Joslyn", "Joy", "Joyce", "Juanita", "Judith", "Judy", "Julia", "Juliana", "Julie", "Juliet", "Juliette", "June", "Justice", "Kaitlin", "Kaitlyn", "Kali", "Kamryn", "Kara", "Karen", "Karina", "Karissa", "Karla", "Karly", "Katelyn", "Katherine", "Kathleen", "Kathy", "Katie", "Katlyn", "Katrina", "Kayla", "Kaylee", "Kayleigh", "Kaylie", "Kaylin", "Kelly", "Kelsey", "Kendall", "Kendra", "Kenna", "Kennedy", "Keri", "Kerri", "Kerry", "Kesha", "Keturah", "Kia", "Kiara", "Kimberly", "Kimmy", "Kinsey", "Kirsten", "Kirstin", "Kita", "Kitty", "Kiya", "Kourtney", "Kristen", "Kristi", "Kristie", "Kristin", "Krystal", "Lacey", "Lacy", "Ladonna", "Lainey", "Lakeisha", "Lakisha", "Laken", "Lam", "Lana", "Lanai", "Lani", "Lanisha", "Laquita", "Lara", "Larissa", "Laurel", "Lauren", "Laurie", "Laverne", "Lavinia", "Layla", "Leah", "Lea"};
        cus.createProperty("ID", OType.INTEGER);
        cus.createProperty("Name", OType.STRING);
        cus.createProperty("Age", OType.INTEGER);
        cus.createProperty("PhoneNumber", OType.STRING);
        cus.createProperty("Address", OType.STRING);
        cus.createIndex("Customer.ID", OClass.INDEX_TYPE.UNIQUE, "ID");

        for (int i = 0; i < 800; i++) {
            OElement cus_obj = db.newElement("Customer");
            cus_obj.setProperty("ID", i);
            cus_obj.setProperty("Name", name[i]);
            Random random = new Random();
            int age = random.nextInt(70 - 10 + 1) + 20;
            cus_obj.setProperty("Age", age);
            String phone = randomPhoneNumber();
            cus_obj.setProperty("PhoneNumber", phone);
            String address = randomAddress();
            cus_obj.setProperty("Address", address);
            cus_obj.save();
        }

        db.getMetadata().getSchema().dropClass("Customer_Index");
        OClass cus_index = db.createVertexClass("Customer_Index");
        cus_index.createProperty("ID", OType.INTEGER);
        cus_index.createProperty("Name", OType.STRING);
        cus_index.createProperty("Age", OType.INTEGER);
        cus_index.createProperty("PhoneNumber", OType.STRING);
        cus_index.createProperty("Address", OType.STRING);
        cus_index.createIndex("Customer_Index.ID", OClass.INDEX_TYPE.UNIQUE, "ID");

        try (OResultSet rs = db.query("SELECT * FROM Customer")) {
            while (rs.hasNext()) {
                OResult row = rs.next();
                OElement cus_obj = db.newElement("Customer_Index");
                cus_obj.setProperty("ID", row.getProperty("ID"));
                cus_obj.setProperty("Name", row.getProperty("Name"));
                cus_obj.setProperty("Age", row.getProperty("Age"));
                cus_obj.setProperty("PhoneNumber", row.getProperty("PhoneNumber"));
                cus_obj.setProperty("Address", row.getProperty("Address"));
                cus_obj.save();
            }
        }

        db.getClass("Customer_Index").getProperty("Age").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

        long startTime_index = System.nanoTime();
        try (OResultSet rs = db.query("SELECT * FROM Customer_Index where Age = 30")) {
            while (rs.hasNext()) {
                OResult row = rs.next();
                System.out.println("ID: " + row.getProperty("ID"));
                System.out.println("Name: " + row.getProperty("Name"));
                System.out.println("Age: " + row.getProperty("Age"));
                System.out.println("Phone Number: " + row.getProperty("PhoneNumber"));
                System.out.println("Address: " + row.getProperty("Address"));
            }
        }

        long endTime_index = System.nanoTime();
        double timeElapsed_index = (endTime_index - startTime_index) / 1.0E9;
        System.out.println("Thời gian chạy CÓ Index: " + timeElapsed_index + " giây");

        long startTime = System.nanoTime();
        try (OResultSet rs = db.query("SELECT * FROM Customer where Age = 30")) {
            while (rs.hasNext()) {
                OResult row = rs.next();
                System.out.println("ID: " + row.getProperty("ID"));
                System.out.println("Name: " + row.getProperty("Name"));
                System.out.println("Age: " + row.getProperty("Age"));
                System.out.println("Phone Number: " + row.getProperty("PhoneNumber"));
                System.out.println("Address: " + row.getProperty("Address"));
            }
        }

        long endTime = System.nanoTime();
        double timeElapsed = (endTime - startTime) / 1.0E9;
        System.out.println("Thời gian chạy KHÔNG có Index: " + timeElapsed + " giây");


        db.close();
        orientDB.close();
    }
}
