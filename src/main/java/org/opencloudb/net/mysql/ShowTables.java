package org.opencloudb.net.mysql;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatSystem;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.FrontSession;
import org.opencloudb.util.StringUtil;

/**
 * show tables impl
 * @author yanglixue
 *
 */
public class ShowTables { 

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    private static final String SCHEMA_KEY = "schemaName";
	
	/**
	 * response method.
	 * @param session
	 */
	public static void response(FrontSession session,String stmt) { 
	
        Map<String,String> parm = buildFields(session,stmt);
	  
        ByteBuf buffer = session.allocate(512,1024*4);

        // write header
        buffer = header.write(buffer);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer);
        }

        // write eof
        buffer = eof.write(buffer);

        // write rows
        byte packetId = eof.packetId;
        MycatConfig conf = MycatSystem.getInstance().getConfig(); 
        
        Map<String, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(session.getConInfo().getUser());
        if (user != null) {
	        TreeSet<String> tableSet = new TreeSet<String>();
	        
	        Map<String, SchemaConfig> schemas = conf.getSchemas();
	        for (String name:schemas.keySet()){
	        	if (null !=parm.get(SCHEMA_KEY) && parm.get(SCHEMA_KEY).toUpperCase().equals(name.toUpperCase())  ){

	        		if(null==parm.get("LIKE_KEY")){
	        			tableSet.addAll(schemas.get(name).getTables().keySet());
	        		}else{
	        			String p = "^" + parm.get("LIKE_KEY").replaceAll("%", ".*");
	        			Pattern pattern = Pattern.compile(p,Pattern.CASE_INSENSITIVE);
	        			Matcher ma ;
	        			
	        			for (String tname : schemas.get(name).getTables().keySet()){
	        				ma=pattern.matcher(tname);
	        				if(ma.matches()){
	        					tableSet.add(tname);
	        				}
	        			}
	        			
	        		}
	        		
	        	}
	        };
	        
	        String charset=session.getConInfo().getCharset();
	        
	        for (String name : tableSet) {
		        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		        row.add(StringUtil.encode(name.toLowerCase(), charset));
		        row.packetId = ++packetId;
		        buffer = row.write(buffer);
	        } 
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer);

        // post write
        session.write(buffer);
		
		
    }


	/**
	 * build fields
	 * @param c
	 * @param stmt
	 */
	private static Map<String,String> buildFields(FrontSession c,String stmt) {
	 
		Map<String,String> map = new HashMap<String, String>();
		
		String p1 = "^\\s*(SHOW)\\s+(TABLES)(\\s+(FROM)\\s+([a-zA-Z_0-9]+))?(\\s+(LIKE\\s+'(.*)'))?\\s*";
		
		Pattern pattern = Pattern.compile(p1,Pattern.CASE_INSENSITIVE);
		Matcher ma = pattern.matcher(stmt);

		if(ma.find()){
			  String schemaName=ma.group(5);
			  if (null !=schemaName && (!"".equals(schemaName)) && (!"null".equals(schemaName))){
				  map.put(SCHEMA_KEY, schemaName);
			  }
			  
			 String like = ma.group(8);
			 if (null !=like && (!"".equals(like)) && (!"null".equals(like))){
				  map.put("LIKE_KEY", like);
			  }
			}


		if(null==map.get(SCHEMA_KEY)){
			map.put(SCHEMA_KEY, c.getConInfo().getSchema());
		}
		 
		
		int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("Tables in " + map.get(SCHEMA_KEY), Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
         
        return  map;
        
	}

	
}
