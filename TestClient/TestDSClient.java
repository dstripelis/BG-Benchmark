package orientDB;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;


public class TestDSClientA extends DB {

	private ODatabaseDocumentTx db = null;
	private OClass users = null;
	private OClass resources = null;
	private OClass manipulations = null;
	private int massiveInsertFlag = 0;
	private ODocument newDocument = null;

	private static final int SUCCESS = 0;
	private static final int ERROR = 1;
	
	public boolean init() throws DBException {
		this.db = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		try{
			this.users = this.db.getMetadata().getSchema().getClass("users");
			this.resources = this.db.getMetadata().getSchema().getClass("resources");
			this.manipulations = this.db.getMetadata().getSchema().getClass("manipulations");
		}catch(Exception e) {
			System.out.println("Schema does not exist already..");
		}
		this.newDocument = new ODocument();
		this.db.declareIntent(new OIntentMassiveInsert());
				
		return true;
	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		// TODO Auto-generated method stub
		this.db.activateOnCurrentThread();
		this.newDocument.reset();
		this.newDocument.setClassName(entitySet);
		Integer id = Integer.parseInt(entityPK);
		if (entitySet.equals("users")) {
			this.newDocument.field("userid", id);
			this.newDocument.field("ConfFriends", new ArrayList<Integer>());
			this.newDocument.field("PendFriends", new ArrayList<Integer>());
		}
		else if (entitySet.equals("resources")) {
			this.newDocument.field("rid", id);
		}
		else if (entitySet.equals("manipulations")) {
			this.newDocument.field("mid", id);
		}
		for(String k: values.keySet()) {
			if(!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic")))
				this.newDocument.field(k, values.get(k).toString());
			else
				this.newDocument.field(k, values.get(k).toArray());
		}
		this.newDocument.save();
		return SUCCESS;
	}

	@Override
	public int CreateFriendship(int invitorID, int inviteeID) {
		// TODO Auto-generated method stub
		this.db.activateOnCurrentThread();
		ODocument invitor = (ODocument) db.query(new OSQLSynchQuery<ODocument>("select * from users where userid = " + new Integer(invitorID).toString())).get(0);
		ODocument invitee = (ODocument) db.query(new OSQLSynchQuery<ODocument>("select * from users where userid = " + new Integer(inviteeID).toString())).get(0); 
		
		ArrayList<Integer> confFriends = invitor.field("ConfFriends");
		ArrayList<Integer> pendFriends = invitor.field("PendFriends");
		confFriends.add(new Integer(inviteeID));
		pendFriends.remove(new Integer(inviteeID));
		invitor.field("PendFriends", pendFriends);
		invitor.field("ConfFriends", confFriends);
		this.db.save(invitor);
		confFriends = invitee.field("ConfFriends");
		pendFriends = invitee.field("PendFriends");
		confFriends.add(new Integer(invitorID));
		pendFriends.remove(new Integer(invitorID));
		invitee.field("PendFriends", pendFriends);
		invitee.field("ConfFriends", confFriends);
		this.db.save(invitee);
		//OClass users = this.db.
		return SUCCESS;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		this.db.activateOnCurrentThread();
		ODocument invitee = (ODocument) db.query(new OSQLSynchQuery<ODocument>("select * from users where userid = " + new Integer(inviteeID).toString())).get(0); 		
		
		ArrayList<Integer> pendFriends = invitee.field("PendFriends");
		pendFriends.add(new Integer(inviterID));
		invitee.field("PendFriends", pendFriends);
		this.db.save(invitee);
		return SUCCESS;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		ODatabaseDocumentTx database = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		
		ODocument profileOwner = (ODocument) database.query(new OSQLSynchQuery<ODocument>("select * from users where userid = " + new Integer(profileOwnerID).toString())).get(0);
		ArrayList<Integer> confFriends = profileOwner.field("ConfFriends");
		Integer confFriendCount = confFriends.size();
		ArrayList<Integer> pendFriends = profileOwner.field("PendFriends");
		Integer pendFriendCount = pendFriends.size();
		Long resourceCount = (Long) ((ODocument)database.query(new OSQLSynchQuery<ODocument>("select count(*) from resources where walluserid = " + new Integer(profileOwnerID).toString())).get(0)).field("count");
		profileOwner.removeField("ConfFriends");
		profileOwner.removeField("PendFriends");
		String tmp = profileOwner.field("address");
		result.put("address", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("dob").toString();
		result.put("dob", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("email").toString();
		result.put("email", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("fname").toString();
		result.put("fname", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("gender").toString();
		result.put("gender", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("jdate").toString();
		result.put("jdate", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("ldate").toString();
		result.put("ldate", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("lname").toString();
		result.put("lname", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("tel").toString();
		result.put("tel", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("userid").toString();
		result.put("userid", new ObjectByteIterator(tmp.getBytes()));
		tmp = profileOwner.field("username").toString();
		result.put("username", new ObjectByteIterator(tmp.getBytes()));
		result.put("friendcount", new ObjectByteIterator(Integer.toString(confFriendCount).getBytes()));
		if(requesterID == profileOwnerID) {
			result.put("pendingcount", new ObjectByteIterator(Integer.toString(pendFriendCount).getBytes()));
		}
		if (insertImage) {
			result.put("pic", new ObjectByteIterator(profileOwner.field("pic").toString().getBytes()));
		}
		System.out.println("done..");
		result.put("resourcecount", new ObjectByteIterator(Long.toString(resourceCount).getBytes()));
		database.close();
		return SUCCESS;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub
				
		// populate fields if empty		
		if (fields.size() == 0){
			fields = new HashSet<String> (Arrays.asList("username", "pw",
					"fname", "lname", "gender", "dob", "jdate", "ldate",
					"address", "email", "tel"));
		}
		// if insertImage is True fetch the thumb-nail picture
		if (insertImage) fields.add("tpic");
		
		// concatenate the fields for the Select Statement
		String attributes = String.join(",",fields);			
					
		if (requesterID < 0 || profileOwnerID < 0) {
			return ERROR; 
		}
			
		// Get profileOwner Friends
		ODocument profileOwnerFriends = (ODocument) this.db.query(new OSQLSynchQuery<ODocument>("select ConfFriends from users where userid = " + new Integer(profileOwnerID).toString())).get(0);
		ArrayList<Integer> ConfFriends = profileOwnerFriends.field("ConfFriends"); 

		// Prepared Query
		OSQLSynchQuery<ODocument> user_query = new OSQLSynchQuery<ODocument>("select " + attributes.toString() + " from users where userid = ?");
		
		// Save Query response 
		List<ODocument> user_data;
				
		// Iterate through Friends List
		for (Integer friendID : ConfFriends){
			// Store values for each user 
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			values.put("userid", new ObjectByteIterator(String.valueOf(friendID).getBytes()));
			user_data = this.db.command(user_query).execute(friendID);
			for (String attribute : fields){				
				values.put(attribute, new ObjectByteIterator(String.valueOf(user_data.get(0).field(attribute).toString()).getBytes()));					
			}
			result.add(values);			
		}		
		return SUCCESS;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
				
		// populate fields
		Set<String> fields = new HashSet<String> (Arrays.asList("username", "pw",
				"fname", "lname", "gender", "dob", "jdate", "ldate",
				"address", "email", "tel")); 
		
		// if insertImage is True fetch the thumb-nail picture
		if (insertImage) fields.add("tpic");
		
		// concatenate the fields for the Select Statement
		String attributes = String.join(",",fields);			
					
		if (profileOwnerID < 0) {
			return ERROR; 
		}
			
		// Get profileOwner Friends
		ODocument profileOwnerPendingRequests = (ODocument) db.query(new OSQLSynchQuery<ODocument>("select PendFriends from users where userid = " + new Integer(profileOwnerID).toString())).get(0);
		ArrayList<Integer> PendFriends = profileOwnerPendingRequests.field("PendFriends"); 

		// Prepared Query
		OSQLSynchQuery<ODocument> user_query = new OSQLSynchQuery<ODocument>("select " + attributes.toString() + " from users where userid = ?");
		
		// Save Query response 
		List<ODocument> user_data;
				
		// Iterate through Pending Friends List
		for (Integer friendID : PendFriends){
			// Store values for each user 
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			values.put("userid", new ObjectByteIterator(String.valueOf(friendID).getBytes()));
			user_data = db.command(user_query).execute(friendID);
			for (String attribute : fields){				
				values.put(attribute, new ObjectByteIterator(String.valueOf(user_data.get(0).field(attribute).toString()).getBytes()));					
			}
			results.add(values);
		}		
		return SUCCESS;			
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		if (inviterID <0 || inviteeID<0){
			return ERROR;
		}		
		int res1 = CreateFriendship(inviterID,inviteeID);
		
		if ( res1 == ERROR ){
			return ERROR;
		}
		return SUCCESS;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		if (inviterID <0 || inviteeID<0){
			return ERROR;
		}
		db.command(new OCommandSQL("UPDATE users REMOVE PendFriends=? WHERE userid=?")).execute(inviterID,inviteeID);
		return SUCCESS;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		ODatabaseDocumentTx database = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		List<ODocument> ownerResources = (List<ODocument>) database.query(new OSQLSynchQuery<ODocument>("select * from resources where walluserid = " + new Integer(profileOwnerID).toString() + " order by rid DESC LIMIT " + new Integer(k).toString()));
		for(ODocument res : ownerResources) {
			HashMap<String, ByteIterator> resMap = new HashMap<String, ByteIterator>();
			for(String fname : res.fieldNames()) {
				resMap.put(fname, (ByteIterator)res.field(fname));
			}
			result.add(resMap);
		}
		return SUCCESS;
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		ODatabaseDocumentTx database = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		List<ODocument> ownerResources = (List<ODocument>) database.query(new OSQLSynchQuery<ODocument>("select * from resources where walluserid = " + new Integer(creatorID).toString() + " order by rid DESC"));
		for(ODocument res : ownerResources) {
			HashMap<String, ByteIterator> resMap = new HashMap<String, ByteIterator>();
			for(String fname : res.fieldNames()) {
				resMap.put(fname, (ByteIterator)res.field(fname));
			}
			result.add(resMap);
		}
		return SUCCESS;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		ODatabaseDocumentTx database = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		List<ODocument> resourceManipulations = (List<ODocument>) database.query(new OSQLSynchQuery<ODocument>("select * from manipulations where rid = " + new Integer(resourceID).toString()));
		for(ODocument res : resourceManipulations) {
			HashMap<String, ByteIterator> resMap = new HashMap<String, ByteIterator>();
			for(String fname : res.fieldNames()) {
				resMap.put(fname, (ByteIterator)res.field(fname));
			}
			result.add(resMap);
		}
		return SUCCESS;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		ODatabaseDocumentTx database = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		database.activateOnCurrentThread();
		ODocument newDoc = new ODocument();
		newDoc.setClassName("manipulations");
		Integer id = (Integer)(Object)(values.get("mid"));
		newDoc.field("mid", id);
		newDoc.field("creatorid",new ObjectByteIterator(Integer.toString(resourceCreatorID).getBytes()));
		newDoc.field("rid", new ObjectByteIterator(Integer.toString(resourceID).getBytes()));
		newDoc.field("modifierid", new ObjectByteIterator(Integer.toString(commentCreatorID).getBytes()));
		newDoc.field("timestamp",values.get("timestamp"));
		newDoc.field("type", values.get("type") );
		newDoc.field("content", values.get("content"));
		newDoc.save();
		return SUCCESS;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		if (resourceCreatorID < 0 || resourceCreatorID < 0 || resourceID < 0){
			return ERROR;
		}		
		db.command(new OCommandSQL("DELETE FROM resources WHERE rid=? AND creatorid=? AND walluserid=?")).execute(resourceCreatorID, resourceID, manipulationID);		
		return SUCCESS;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub		
		if (friendid1 < 0 || friendid2 < 0){
			return ERROR;
		}		
		// delete friendid1 from friendid2
		db.command(new OCommandSQL("UPDATE users REMOVE ConfFriends=? WHERE userid=?")).execute(friendid1,friendid2);
		// delete friendid2 from friendid1
		db.command(new OCommandSQL("UPDATE users REMOVE ConfFriends=? WHERE userid=?")).execute(friendid2,friendid1);						
		return SUCCESS;
	}

	public void createIndexes() {
		System.out.println("creating indexes");
		this.users.createIndex("userIndex", OClass.INDEX_TYPE.UNIQUE, "userid");
		this.resources.createIndex("resourceIndex", OClass.INDEX_TYPE.UNIQUE, "rid");
		this.resources.createIndex("resourceWallUserIDIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "walluserid");
		this.resources.createIndex("resourceCreatorUserIDIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "creatorid");
		this.manipulations.createIndex("manipulationIndex", OClass.INDEX_TYPE.UNIQUE, "mid");
		this.manipulations.createIndex("manipulationResourceIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "rid");
		this.manipulations.createIndex("manipulationCreatorIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "creatorid");
		this.manipulations.createIndex("manipulationModifierIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "modifierid");
		System.out.println("done creating indexes");

	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub		
		this.db.declareIntent(null);
								
		// Total Users
		ODocument ucount = (ODocument) this.db.query(new OSQLSynchQuery<ODocument>("select count(*) from users")).get(0);
		int usercount = ((Long) ucount.field("count")).intValue();
		
		// Get Statistics of 1st user 
		ODocument user = (ODocument) this.db.query(new OSQLSynchQuery<ODocument>("select * from users limit 1")).get(0);
		int uid = (int) user.field("userid");		
								
		// Confirmed Friends
		ArrayList<Integer> userConfFriendsList = user.field("ConfFriends");
		int user_total_ConfFriends = userConfFriendsList.size();
		
		// Pending Friends
		ArrayList<Integer> userPendFriendsList = user.field("PendFriends");
		int user_total_PendFriends = userPendFriendsList.size();
		
		// Get resources count for the specific user id
		ODocument user_resources = (ODocument) this.db.query(new OSQLSynchQuery<ODocument> ("select count(rid) from resources where creatorid = " + new Integer (uid).toString())).get(0);
		
		// User Resources					
		int user_total_Resources = ((Long) user_resources.field("count")).intValue();

		double avgfriendsperuser = user_total_ConfFriends;
		double avgpendingperuser = user_total_PendFriends;
		double resourcesperuser = user_total_Resources;
		
		HashMap <String,String> hash = new HashMap<String,String>();
		hash.put("usercount", Integer.toString(usercount));
		hash.put("avgfriendsperuser", Double.toString(avgfriendsperuser));
		hash.put("avgpendingperuser", Double.toString(avgpendingperuser));
		hash.put("resourcesperuser", Double.toString(resourcesperuser));

		return hash;
	}

	@Override
	public void createSchema(Properties props) {
		// TODO Auto-generated method stub
		this.createSchemaForUsers();
		this.createSchemaForResources();
		this.createSchemaForManipulations();
		this.createIndexes();
		this.db.command(new OSQLSynchQuery("ALTER database DATETIMEFORMAT" + " 'YYYY/MM/dd HH:MM:SS' "));
		this.db.close();
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		if (memberID<0) {
			return ERROR;
		}		
		ODocument memberPendFriends = (ODocument) db.query(new OSQLSynchQuery<ODocument>("select PendFriends from users where userid = " + new Integer(memberID).toString())).get(0);
		ArrayList<Integer> PendFriendsList = memberPendFriends.field("PendFriends");
		pendingIds = new Vector<Integer>(PendFriendsList); 		
		return SUCCESS;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub		
		if (memberID<0) {
			return ERROR;
		}		
		ODocument memberConfFriends = (ODocument) db.query(new OSQLSynchQuery<ODocument>("select ConfFriends from users where userid = " + new Integer(memberID).toString())).get(0);
		ArrayList<Integer> ConfFriendsList = memberConfFriends.field("ConfFriends");		
		confirmedIds = new Vector<Integer>(ConfFriendsList); 
				
		return SUCCESS;
	}

	public void createSchemaForUsers() {
		this.users = this.db.getMetadata().getSchema().createClass("users");
		this.users.createProperty("userid", OType.INTEGER);
		this.users.createProperty("username", OType.STRING);
		this.users.createProperty("pw", OType.STRING);
		this.users.createProperty("fname", OType.STRING);
		this.users.createProperty("lname", OType.STRING);
		this.users.createProperty("gender", OType.STRING);
		this.users.createProperty("dob", OType.DATE);
		this.users.createProperty("jdate", OType.DATE);
		this.users.createProperty("ldate", OType.DATE);
		this.users.createProperty("address", OType.STRING);
		this.users.createProperty("email", OType.STRING);
		this.users.createProperty("tel", OType.STRING);
	}

	public void createSchemaForResources() {
		this.resources = this.db.getMetadata().getSchema().createClass("resources");
		this.resources.createProperty("rid", OType.INTEGER);
		this.resources.createProperty("creatorid", OType.INTEGER);
		this.resources.createProperty("walluserid", OType.INTEGER);
		this.resources.createProperty("type", OType.STRING);
		this.resources.createProperty("body", OType.STRING);
		this.resources.createProperty("doc", OType.STRING);
	}

	public void createSchemaForManipulations() {
		this.manipulations = this.db.getMetadata().getSchema().createClass("manipulations");
		this.manipulations.createProperty("mid", OType.INTEGER);
		this.manipulations.createProperty("creatorid", OType.INTEGER);
		this.manipulations.createProperty("rid", OType.INTEGER);
		this.manipulations.createProperty("modifierid", OType.INTEGER);
		this.manipulations.createProperty("timestamp", OType.DATETIME);
		this.manipulations.createProperty("type", OType.STRING);
		this.manipulations.createProperty("content", OType.STRING);
	}

}
