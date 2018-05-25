package com.ledel.hbase;

import java.io.*;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A class that returns every midpoint of every region
 * of a given HBase table. That will allow to split
 * a specific region because you need a split point
 * 
 * Usage: hadoop jar <jarname> <tablename>
 * or export jar in hbase client classpath and: hbase com.ledel.hbase.GetMidPoint <TABLENAME>
 */
public class GetMidPoint {
	private static final Log LOG = LogFactory.getLog(GetMidPoint.class);
	
    public static void main(String[] args) throws IOException, InterruptedException {
    	
        if(args.length == 0) {
        		LOG.fatal("No table name provided. Please run with hadoop jar xxx.jar TABLE [COLUMNFAMILY]");
        		LOG.fatal("or: ");
        		LOG.fatal("export HBASE_CLASSPATH=${HBASE_CLASSPATH}:./hbase-getmidpoint.jar"); 
        		LOG.fatal("hbase com.ledel.hbase.GetMidPoint TABLE [COLUMNFAMILY]");
            System.exit(0);
        }
        
        TableName TABLE_NAME = TableName.valueOf(args[0]);
        String COLUMN_FAMILY = args.length == 2 ? args[1] : null;
        
    		PrintStream out = new PrintStream(new FileOutputStream("splitpoints.csv"));
    		System.setOut(out);
        Configuration config = HBaseConfiguration.create();
        
        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();
        
        if (!admin.tableExists(TABLE_NAME)) {
    		LOG.fatal("Table " + args[0] + "does not exists");
            System.exit(0);        		
        }
        
        Table table = conn.getTable(TABLE_NAME);
        List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);

		System.out.println("REGION NAME,SPLIT POINT,STORE SIZE,COLUMN FAMILY");
		
        for (HRegionInfo hRegionInfo : regions) {
        		LOG.info("Full region name: " + hRegionInfo.getRegionNameAsString());
			HRegion region = HRegion.openHRegion(hRegionInfo, table.getTableDescriptor(), null, config);
			List<Store> stores = region.getStores();
			LOG.info(stores.size() + " stores (Column Families) found: " + stores.toString());
    			
			for (Store store : stores) {
				if(COLUMN_FAMILY == null || (COLUMN_FAMILY != null && COLUMN_FAMILY.equals(store.getColumnFamilyName()))) {
		        		byte[] splitPoint = store.getSplitPoint();
		        		String split = Bytes.toString(splitPoint);
		        		System.out.println(store.getRegionInfo().getEncodedName() 
		        				+ "," + split
		        				+ "," + store.getSize()
		        				+ "," + store.getColumnFamilyName());
				}
			}
		}
        
        table.close();
	    	admin.close();
    }
}
