package mongodb;

import java.net.UnknownHostException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class mongoexe {
	public static void main	(String[] args){
		try {
			 
			// CONECTAR CON MONGODB
			MongoClient mongo = new MongoClient("localhost", 27017);
		 
			// SELECCIONA LA BASE DE DATOS (Si no existe MongoDB la crea)
			DB db = mongo.getDB("testdb");
		 
			// SELECCIONA TABLA (Si no existe MongoDB la crea)
			DBCollection table = db.getCollection("user");
			
			// Limpia datos
			table.remove(new BasicDBObject());
			
			// INSERT 
			// primer docuemnto
			BasicDBObject document = new BasicDBObject();
			document.put("id", 1);
			document.put("name", "Pedro Perez");
			document.put("age", 30);
			document.put("created", new Date());
			table.insert(document);
			
			// segundo documento
			document = new BasicDBObject();
			document.put("id", 2);
			document.put("name", "Diego M");
			document.put("age", 29);
			document.put("createdDate", new Date());
			table.insert(document);
			
			// tercer documento
			document = new BasicDBObject();
			document.put("id", 3);
			document.put("name", "Mario Bros");
			document.put("age", 40);
			document.put("createdDate", new Date());
			table.insert(document);
		 
			// Imprime datos actuales
			System.out.println("*** INSERT *** ");
			PrintData(table);
			
			// SELECT DATOS CON CONDICION 
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("name", "Mario Bros");
		 
			DBCursor cursor = table.find(searchQuery);
		 
			System.out.println("*** SELECT (query) ***");
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			
			// SELECT TODOS LOS DATOS
			cursor = table.find();
			System.out.println("*** SELECT (all data) ***");
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
		 
			// UPDATE 
			BasicDBObject query = new BasicDBObject();
			query.put("name", "Mario Bros");
		 
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.put("age", "44");
			newDocument.put("name", "Luigi Bros");
		 
			BasicDBObject updateObj = new BasicDBObject();
			updateObj.put("$set", newDocument);
		 
			table.update(query, updateObj);

			// Imprime datos actuales
			System.out.println("*** UPDATE *** ");
			PrintData(table);		
			
			// DELETE
			BasicDBObject del = new BasicDBObject().append("id", 1);
			table.remove(del);
			
			// Imprime datos actuales
			System.out.println("*** DELETE *** ");
			PrintData(table);
			
			// FIN
			System.out.println("FIN");
		 
	    } catch (UnknownHostException e) {
	    	e.printStackTrace();
	    } catch (MongoException e) {
	    	e.printStackTrace();
	    }
	}
	/**
	 * Imprime todos los datos de una coleccion (tabla)
	 * @param table
	 */
	public static void PrintData(DBCollection table){
		DBCursor cursor = table.find();
		 
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
}
