import java.io.*;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import org.bson.Document;
import com.mongodb.client.MongoClients;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Iterator;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.MongoWriteException;
import org.apache.commons.io.IOUtils;
import org.bson.json.JsonObject;

import javax.naming.StringRefAddr;

public class mongo {
    public static void main(String[] args) throws IOException {

        com.mongodb.client.MongoClient client = MongoClients.create("<MongoDB Atlas Connection String>")

        MongoDatabase database = client.getDatabase("rita");
        MongoCollection<org.bson.Document> coll = database.getCollection("test");
        JSONParser parser = new JSONParser();
        try {
            coll.drop();


            //Insert Many Approach:
           // profilelight.json 1 json document per line in file
            InputStream inStream = new FileInputStream("src/main/resources/profilelight.json");

            List<Document> ldocs = IOUtils.readLines(inStream, StandardCharsets.UTF_8.name()).stream().map(Document::parse).collect(Collectors.toList());

            //###insertMany in 4.2.2 is void
            coll.insertMany(ldocs);
            System.out.println("Inserted");


            //Bulk  Approach:
            // profilelight.json 1 json document per line in file
/*              int count = 0;
                int batch = 100;

                List<InsertOneModel<Document>> docs = new ArrayList<>();

                try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/profilelight.json"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        docs.add(new InsertOneModel<>(Document.parse(line)));
                        count++;
                        if (count == batch) {
                            coll.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                            docs.clear();
                            count = 0;
                        }
                    }
                }

                if (count > 0) {
                    BulkWriteResult bulkWriteResult=  coll.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                    System.out.println("Inserted" + bulkWriteResult);
                }
*/


            //Array Json Approach - NOT OPTIMAL OR RECOMMENDED:
            //arrayjson_sample.json, file has a Array of jsons, 1 json document per position in the Array


/*                Object obj = parser.parse(new FileReader("src/main/resources/arrayjson_sample.json"));
                // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
                JSONArray importedjson = (JSONArray) obj;

                Iterator<JSONObject> iterator = importedjson.iterator();
                while (iterator.hasNext()) {
                    String sjson = iterator.next().toJSONString();
                    System.out.println(sjson);
                    Document doc = Document.parse(sjson);
                    coll.insertOne(doc);
                }*/


        } catch (Exception e) {
            System.out.println("Error" + e);
        }

    }
}