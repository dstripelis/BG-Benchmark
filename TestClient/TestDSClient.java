package orientDB;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;


public class TestDSClientA extends DB {
	
	ODatabaseDocumentTx db = null;
	OClass users = null;
	OClass resources = null;
	OClass manipulations = null;
	
	public boolean init() throws DBException {
		this.db = new ODatabaseDocumentTx("remote:localhost/testDB").open("admin", "admin");
		//ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseDocumentInternal) this.db.getUnderlying());
		return true;
	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		// TODO Auto-generated method stub
		this.db.activateOnCurrentThread();
		//System.out.println("dsfsdfsdf");
		ODocument newDocument = new ODocument(entitySet);
		Integer id = Integer.parseInt(entityPK);
		if (entitySet.equals("users")) {
			newDocument.field("userid", id);
			newDocument.field("ConfFriends", new ArrayList<Integer>());
			newDocument.field("PendFriends", new ArrayList<Integer>());
		}
		else if (entitySet.equals("resources")) {
			newDocument.field("rid", id);
		}
		else if (entitySet.equals("manipulations")) {
			newDocument.field("mid", id);
		}
		for(String k: values.keySet()) {
			if(!(k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic")))
				newDocument.field(k, values.get(k).toString());
		}
		//System.out.println("working");
		this.db.save(newDocument);
		return 0;
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
		return 0;
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
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		// Find Total Users Count 
		ODocument ucount = (ODocument) this.db.query(new OSQLSynchQuery<ODocument>("select count(*) from users")).get(0);
		int usercount = ((Long) ucount.field("count")).intValue();
		
		// Auxiliary collection for Average Confirmed Friends|Pending Friends
		List<ODocument> Ouids = this.db.query(new OSQLSynchQuery<ODocument>("select userid from users"));
		int uid;
		
		// Find Average Friends per User
		int ConfFriendsSum = 0;
		List<ODocument> user_friends;
		OSQLSynchQuery<ODocument> confirmed_query = new OSQLSynchQuery<ODocument>("select ConfFriends from users where userid= ?");
		for	(ODocument Ouid: Ouids){
			uid = (Integer) Ouid.field("userid");
			user_friends = this.db.command(confirmed_query).execute(uid);
			ConfFriendsSum += (int) ((ArrayList<Integer>) user_friends.get(0).field("ConfFriends")).size();			
		}
		double avgfriendsperuser = ConfFriendsSum/usercount;
	
		
		// Find Average Pending per User
		int PendFriendsSum = 0;
		List<ODocument> user_pendings;
		OSQLSynchQuery<ODocument> pendings_query = new OSQLSynchQuery<ODocument>("select PendFriends from users where userid= ?");
		for	(ODocument Ouid: Ouids){
			uid = (Integer) Ouid.field("userid");
			user_pendings = this.db.command(pendings_query).execute(uid);
			PendFriendsSum += (int) ((ArrayList<Integer>) user_pendings.get(0).field("PendFriends")).size();			
		}
		double avgpendingperuser = PendFriendsSum/usercount;				
		
		
		// Find Average Resources per User		
		int ResourcesSum = 0;
		List<ODocument> user_resources;
		OSQLSynchQuery<ODocument> resources_query = new OSQLSynchQuery<ODocument>("select count(rid) from resources where creatorid = ?");
		for	(ODocument Ouid: Ouids){
			uid = (Integer) Ouid.field("userid");
			user_resources =  this.db.command(resources_query).execute(uid);			
			ResourcesSum += ((Long) user_resources.get(0).field("count")).intValue();						
		}
		double  resourcesperuser = ResourcesSum/usercount;		
	
		
		// ("usercount", "avgfriendsperuser", "avgpendingperuser", "resourcesperuser")
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
		this.db.command(new OSQLSynchQuery("ALTER database DATETIMEFORMAT + 'YYYY/MM/dd HH:MM:SS' "));
		this.db.close();
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public void createSchemaForUsers() {
		this.users = this.db.getMetadata().getSchema().createClass("users");
		this.users.createProperty("userid", OType.INTEGER);
		this.users.createProperty("username", OType.STRING);
		this.users.createProperty("pw", OType.STRING);
		this.users.createProperty("fname", OType.STRING);
		this.users.createProperty("lname", OType.STRING);
		this.users.createProperty("gender", OType.STRING);
		this.users.createProperty("dob", OType.DATETIME);
		this.users.createProperty("jdate", OType.DATETIME);
		this.users.createProperty("ldate", OType.DATETIME);
		this.users.createProperty("address", OType.STRING);
		this.users.createProperty("email", OType.STRING);
		this.users.createProperty("tel", OType.STRING);
		this.users.createIndex("userIndex", OClass.INDEX_TYPE.UNIQUE, "userid");
	}
	
	public void createSchemaForResources() {
		this.resources = this.db.getMetadata().getSchema().createClass("resources");
		this.resources.createProperty("rid", OType.INTEGER);
		this.resources.createProperty("creatorid", OType.INTEGER);
		this.resources.createProperty("walluserid", OType.INTEGER);
		this.resources.createProperty("type", OType.STRING);
		this.resources.createProperty("body", OType.STRING);
		this.resources.createProperty("doc", OType.STRING);
		this.resources.createIndex("resourceIndex", OClass.INDEX_TYPE.UNIQUE, "rid");
		this.resources.createIndex("resourceWallUserIDIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "walluserid");
		this.resources.createIndex("resourceCreatorUserIDIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "creatorid");
	}
	
	public void createSchemaForManipulations() {
		this.manipulations = this.db.getMetadata().getSchema().createClass("manipulations");
		this.manipulations.createProperty("mid", OType.INTEGER);
		this.manipulations.createProperty("creatorid", OType.STRING);
		this.manipulations.createProperty("rid", OType.INTEGER);
		this.manipulations.createProperty("modifierid", OType.INTEGER);
		this.manipulations.createProperty("timestamp", OType.DATETIME);
		this.manipulations.createProperty("type", OType.STRING);
		this.manipulations.createProperty("content", OType.STRING);
		this.manipulations.createIndex("manipulationIndex", OClass.INDEX_TYPE.UNIQUE, "mid");
		this.manipulations.createIndex("manipulationResourceIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "rid");
	}

}
