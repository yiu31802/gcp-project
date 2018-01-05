package com.github.yiu31802.gcpproject;

public class WriteToDatastoreFromMemory {
    public static void main(String[] args) {
        String project_id = (System.getProperty("XGCPID") != null) ? (System.getProperty("XGCPID")) : "";
        System.out.print("here we are:\n" + project_id);
    }
}