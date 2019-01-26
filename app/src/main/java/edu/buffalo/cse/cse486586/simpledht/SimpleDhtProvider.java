package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public class SimpleDhtProvider extends ContentProvider {
    /*
     */
    int port_number=0;
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    public static TreeMap<String, String> chord_ring;
    private HashMap<String,String> single_query = new HashMap();
    private HashMap<String,String> star_queries = new HashMap();
    private static final int SERVER_PORT = 10000;
    private static final String STAR_SYMBOL = "*";
    private static final String AT_SYMBOL = "@";
    private static final String NOTHING = "xxxxx";
    private static final String NEWJOIN = "NEWJOIN";
    private static final String DELETE = "DELETE";
    private static final String DELETE_ALL = "DELETE_ALL";
    private static final String INSERT = "INSERT";
    private static final String QUERY = "QUERY";
    private static final String QUERY_ALL = "QUERY_ALL";
    private static final String QUERY_REPLY = "QUERY_REPLY";
    private static final String QUERY_ALL_REPLY = "QUERY_ALL_REPLY";
    private static final String UPDATE = "UPDATE";
    private static String head;
    private static String pred_hash_id;
    private static String succ_hash_id;
    private static String pred_port_id;
    private static String succ_port_id;
    private static String my_node_id;
    private static String my_port_id;
    private static final String myDelimiter = "###";
    private static String headpred;
    private static String headsucc;
    private final Uri myUri = makeUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private Boolean foundOrigin = false;
    private Boolean deleteSingle = false;
    private Boolean deleteAll = false;
    /*
     */
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
   /* My code starts here*/
        String qType = selection;
        String key_val[] = {"key", "value"};
        String sb = "";
        Log.v("inside delete ","selection is "+ selection);
        // same as query almost
        if(qType.compareTo(STAR_SYMBOL)==0)
        {
            if(pred_hash_id.compareTo(my_node_id)==0)
            {
                // when you are the only avd in the ring.
                // just get all from here.
                try {
                    BufferedReader reader = null;
                    int c = 0;
                    File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                    if (file_path.listFiles() != null) {
                        for (File f : file_path.listFiles()) {
                            if(f.delete())
                            {
                                c ++;
                            }
                        }
                        reader.close();
                    }
                    return c;

                } catch (Exception e) {
                    Log.e("STAR_DELETE1", "file deleting has failed");
                }
            }
            else
            {
                int c = 0;
                try {
                    BufferedReader reader = null;

                    File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                    if (file_path.listFiles() != null) {
                        for (File f : file_path.listFiles()) {
                            if(f.delete())
                            {
                                c ++;
                            }
                        }
                        reader.close();
                    }
                } catch (Exception e) {
                    Log.e("STAR_DELETE :ELSE", "file reading has failed");
                }
                // Send a message to the next one requesting for all messages to be deleted from their AVDs.
                String msgToSucc = "";
                msgToSucc = DELETE_ALL+myDelimiter+qType+myDelimiter+my_port_id;
                sendMsg(msgToSucc, succ_port_id);
                while(deleteAll!=true)
                {}

                return c;
            }
        }
        else if(qType.compareTo(AT_SYMBOL)==0)
        {
            // just get all from here
            try {
                BufferedReader reader = null;
                int c = 0;
                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        if(f.delete())
                        {
                            c ++;
                        }
                    }

                    reader.close();
                }
                return c;
            } catch (Exception e) {
                Log.e("AT_DELETE", "file reading has failed");
            }
        }
        else
        {
            File file = new File(getContext().getFilesDir(),qType);
            if(file.exists()){
                //if current avd has file
                try {
                    if(file.delete())
                    {
                        // only one file to be deleted
                        return 1;
                    }

                } catch (NullPointerException e) {
                    Log.e("inside else ","NPE");
                }
            }
            else{
                // else send message to succ to delete required data
                String msgToSucc = "";
                msgToSucc = DELETE+myDelimiter+qType+myDelimiter+my_port_id;
                sendMsg(msgToSucc, succ_port_id);
                Log.v("delete", "while deleting");
//                while(deleteSingle!=true)
//                {}
//                deleteSingle = false;
            }
        }
        Log.v("Delete end ","selection: "+selection);

   /* My code ends here*/
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        try{
            String key_data = values.get("key").toString();
            Log.v("key is: ", key_data);
            String value_data = values.get("value").toString();
            Log.v("value is: ", value_data);

            String key_hash = genHash(key_data);
            if(my_node_id.equals(pred_hash_id))
            {
                //insert here
                Log.d(TAG, "insert : in this avd");
                FileOutputStream out_stream = getContext().openFileOutput(key_data, Context.MODE_PRIVATE);
                out_stream.write(value_data.getBytes());
                out_stream.close();
            }
            else if(pred_hash_id.compareTo(my_node_id)>0)
            {
                if(  key_hash.compareTo(my_node_id)<=0  ||  key_hash.compareTo(pred_hash_id)>0){
                    // insert here
                    Log.d(TAG, "insert : in this avd");
                    FileOutputStream out_stream = getContext().openFileOutput(key_data, Context.MODE_PRIVATE);
                    out_stream.write(value_data.getBytes());
                    out_stream.close();
                }
                else{
                    // send it to  successor
                    Log.d(TAG, "insert: sending to next avd");
                    String message = INSERT+myDelimiter+key_data+myDelimiter+value_data;
                    sendMsg(message, succ_port_id);

                }
            }
            else if(pred_hash_id.compareTo(my_node_id)<0)
            {
                if(key_hash.compareTo(my_node_id)<=0 && key_hash.compareTo(pred_hash_id)>0)
                {
                    // insert here
                    Log.d(TAG, "insert: in this avd");
                    FileOutputStream out_stream = getContext().openFileOutput(key_data, Context.MODE_PRIVATE);
                    out_stream.write(value_data.getBytes());
                    out_stream.close();

                }
                else{
                    // send it to successor
                    Log.d(TAG, "insert: sending to next avd");
                    String message = INSERT+myDelimiter+key_data+myDelimiter+value_data;
                    sendMsg(message, succ_port_id);

                }

            }

        }
        catch(IOException e){

        }
        catch(NoSuchAlgorithmException e){

        }
        Log.v("insert", values.toString());
        return uri;
        //return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        if(this.getContext()!=null)
        {

            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            my_port_id = myPort;
            port_number = Integer.parseInt(myPort);
            String hash_id="";
            Log.v("in onCreate()", "");
            try{
                Log.v("in onCreate()", "before connecting to server port");
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
                Log.v("in onCreate()", " after connecting to server port");

            }catch(IOException e)
            {
                Log.e("onCreate"," Can't create server sock");

            }
            try{
                hash_id = genHash(portStr);
            }catch(NoSuchAlgorithmException e)
            {
                Log.e("hash prob", "No Such Algo ");
            }
            succ_hash_id = hash_id;
            pred_hash_id = hash_id;
            my_node_id = hash_id;
            head = hash_id;
            headpred = hash_id;
            headsucc = hash_id;
            pred_port_id = my_port_id;
            succ_port_id = my_port_id;
            Log.v("in onCreate()", "My port is "+ myPort+ " and Hash is"+ hash_id);
            if(myPort.compareTo("11108")!=0)
            {
                Log.v("in onCreate()", "port is not 11108");
                String message = "";
                message =NEWJOIN+ myDelimiter +myPort + myDelimiter +""+ myDelimiter + hash_id ;
                Log.v("in onCreate()", "bsending join messg");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, "11108");
                Log.v("in onCreate()", "sending join messg");

            }
            else{
                // tree map because it orders content
                chord_ring = new TreeMap<String, String>(
                        new Comparator<String>(){
                            @Override
                            public int compare(String ele1, String ele2)
                            {
                                return ele1.compareTo(ele2);
                            }
                        }
                );
                chord_ring.put(hash_id, myPort);
                Log.v("in onCreate()", "Putting into ring, port "+ port_number);
            }
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
       /* My code starts here*/
        String qType = selection;
        String key_val[] = {"key", "value"};
        String sb = "";
        Log.v("inside query ","selection is "+ selection);

        if(qType.compareTo(STAR_SYMBOL)==0)
        {
            if(pred_hash_id.compareTo(my_node_id)==0)
            {
                // just get all from here
                try {
                    BufferedReader reader = null;
                    String s = "";
                    MatrixCursor mc = new MatrixCursor(key_val);
                    File file = new File(getContext().getFilesDir().getAbsolutePath());
                    if (file.listFiles() != null) {
                        for (File f : file.listFiles()) {
                            s = f.getName();
                            reader = new BufferedReader(new FileReader(file + "/" + s));
                            String value = reader.readLine();
                            String[] row1 = {
                                    s, value
                            };
                            mc.addRow(row1);
                        }
                        reader.close();
                    }
                    return mc;

                } catch (Exception e) {
                    Log.e("STAR_QUERY1", "file reading has failed");
                }
            }
            else
            {
                // get from here and then send to successor for information
                MatrixCursor mc1 = new MatrixCursor(key_val);
                try {
                    BufferedReader reader = null;
                    String s = "";
                    File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                    if (file_path.listFiles() != null) {
                        for (File f : file_path.listFiles()) {
                            s = f.getName();
                            reader = new BufferedReader(new FileReader(file_path + "/" + s));
                            String value1 = reader.readLine();
                            String[] row2 = {
                                    s, value1
                            };
                            mc1.addRow(row2);
                        }
                        reader.close();
                    }
                } catch (Exception e) {
                    Log.e("STAR_QUERY:ELSE", "file reading has failed");
                }
                // Send a message to the next one requesting to get all messages from their AVDs.
                String msgToSucc = "";
                msgToSucc = QUERY_ALL+myDelimiter+qType+myDelimiter+my_port_id;
                sendMsg(msgToSucc, succ_port_id);
                while(foundOrigin != true)
                {
                    //wait for all the replies from all avds
                }
                MatrixCursor mc = new MatrixCursor(key_val);
                for(String key: star_queries.keySet())
                {
                    String val = star_queries.get(key);
                    String[] row1 = {
                            key, val
                    };
                    mc.addRow(row1);
                }
                // ref :https://developer.android.com/reference/android/database/MergeCursor.html
                MergeCursor finalCursor = new MergeCursor(new Cursor[]{mc1,mc});
                return finalCursor;
            }
        }
        else if(qType.compareTo(AT_SYMBOL)==0)
        {
            // just get all from here
            try {
                BufferedReader reader = null;
                String s = "";
                MatrixCursor mc = new MatrixCursor(key_val);
                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                if (file_path.listFiles() != null) {
                    for (File f : file_path.listFiles()) {
                        s = f.getName();
                        reader = new BufferedReader(new FileReader(file_path + "/" + s));
                        String value = reader.readLine();
                        String[] row1 = {
                                s, value
                        };
                        mc.addRow(row1);
                    }

                    reader.close();
                }
                return mc;
            } catch (Exception e) {
                Log.e("AT_QUERY", "file reading has failed");
            }
        }
        else
        {
            File file = new File(getContext().getFilesDir(),qType);
            MatrixCursor mc = new MatrixCursor(key_val);
            if(file.exists()){
                //if current avd has file
                try {
                    int size;
                    sb="";
                    FileInputStream in_stream = getContext().openFileInput(selection);
                    while ((size = in_stream.read()) != -1) {
                        sb += (char) size;
                        //sb+= Character.toString((char) size);
                    }
                    Log.v("inside query 1","sb"+sb);
                    in_stream.close();
                    String row[] = new String[]{selection,sb};
                    mc.addRow(row);

                } catch (FileNotFoundException e) {
                    Log.e("inside else ","FNE");
                } catch (IOException e) {
                    Log.e("inside else ","IOE");
                } catch (NullPointerException e) {
                    Log.e("inside else ","NPE");
                }
            }
            else{
                // else send message to succ to get required data
                String msgToSucc = "";
                msgToSucc = QUERY+myDelimiter+qType+myDelimiter+my_port_id;
                sendMsg(msgToSucc, succ_port_id);
                while((single_query.get(selection) == null))
                {
                    //if current avd didn't receive the query from successors
                }
                String[] row3 = {
                        selection, single_query.get(selection)
                };
                mc.addRow(row3);

            }
            return mc;
        }
        Log.v("Query end here ","selection: "+selection);
        return null;
       /* My code ends here*/
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private int getNodePosition(String key)
    {
        Log.v("x", "in getNodePosition()");
        int pos = 0;
        for (String cr_key : chord_ring.keySet()) {

            if(cr_key.compareTo(key) == 0)
            {
                break;
            }
            pos++;
        }
        return pos;
    }

    private Uri makeUri(String s, String a) {
        Uri.Builder ub = new Uri.Builder();
        ub.authority(a);
        ub.scheme(s);
        return ub.build();
    }

    private String getSuc(String hashValue, String suc)
    {
        Log.v("x", "in getSuc");
        String theSuc = suc;
        int pos = getNodePosition(theSuc);
        int count  = pos+1;
        int size = chord_ring.size();
        while(hashValue.compareTo(theSuc) > 0)
        {
            pos = getNodePosition(theSuc);
            if(count == size)
            {
                return head;
            }
            theSuc = (String) chord_ring.keySet().toArray()[pos+1];
            count++;
        }
        return theSuc;
    }

    private String getSuccessor(String nodeValue)
    {
        Log.v("x", "in getSuccessor() " + nodeValue);
        String pred;
        int size = chord_ring.size();
        int position = getNodePosition(nodeValue);
        String succ;
        if(position == 0)
        {
           // Log.v("x", "here 1 "+size);
            if(size == 1)
            {
                //Log.v("x", "here 2 "+size);
                succ = nodeValue;
                return succ;
            }
            else{
                //Log.v("x", "here 3 "+size);
                succ = (String) chord_ring.keySet().toArray()[position+1];
            }
        }
        else if((position == (size-1)) && (size!=1))
        {
            //Log.v("x", "here 4 "+size);
            succ = head;
        }
        else{
            //Log.v("x", "in getSuccessor() size is "+size);
            //Log.v("x", "in getSuccessor() position is "+position);
            succ = (String) chord_ring.keySet().toArray()[position+1];
        }
        return succ;
    }

    private String getPredecessor(String nodeValue)
    {
        Log.v("x", "in getPredecessor()");
        String pred;
        int size = chord_ring.size();
        int position = getNodePosition(nodeValue);
        if(position == 0)
        {
            if(size == 1)
            {
                return head;
            }
            else{
                pred = (String) chord_ring.keySet().toArray()[size-1];
            }
        }
        else{
            pred = (String) chord_ring.keySet().toArray()[position-1];
        }
        return pred;
    }

    private String getPortNumber(String hashValue)
    {
        Log.v("x", "in getPortNumber()");
        String portval = chord_ring.get(hashValue);
        return portval;
    }

    private void sendMsg(String message, String remotePort)
    {
        Log.v("x", "in sendMsg, m: "+message+"to port: "+remotePort);
        try {
            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort));
            PrintWriter pw1 = new PrintWriter(socket1.getOutputStream(), true);
            pw1.println(message);
//            socket1.close();

        } catch (UnknownHostException e) {
            Log.d("ClTask", "UnknownHostException");
        } catch (IOException e) {
            Log.d("ClTask", "socketIOException");
        }
    }

    private void printTreeMap()
    {
        if(my_port_id.compareTo("11108")==0) {
            Log.v("ClTask", "Printing Tree Ring Values");
            for (Map.Entry<String, String> entry : chord_ring.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                System.out.printf("%s ::: %s\n", key, value);
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Log.v("CT", "db");
            String message = msgs[0];
            String remotePort = msgs[1];
            Log.v("CT", "msg "+message+" ,remoteport "+ remotePort);
            //sendMsg(message,remotePort);
            try {
                Log.v("CT", "before creating socket");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                Log.v("CT", "Sending join message");
                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                pw.println(message);
                //Log.v("CT", "Before buffer reader");
                Log.v("CT", "I am "+ my_node_id +" my Predecessor is "+ pred_hash_id +" my Successor is "+ succ_hash_id);
                //socket.close();
            } catch (UnknownHostException e) {
                Log.v("ClTask", "UnknownHostException");
            } catch (IOException e) {
                Log.v("ClTask", "socketIOException");
            }
            Log.v("Client Task", "Last Line");
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.v("ST", "DB");
            ServerSocket serverSocket = sockets[0];
            try{
                while(true)
                {
                    Log.v("ST", "1");
                    Socket sock = serverSocket.accept();
                    Log.v("ST", "2");
                    BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    Log.v("ST", "3");
                    String ms = br.readLine();
                    Log.v("ST", "4");
                    String[] messages = ms.split(myDelimiter);
                    // { msgKey/port, msgValue, hash_value, msgType }
                    Log.v("ST", "Received message "+ messages[0]);
                    String  msg_key, msg_value, hash_value, msg_type;
                     msg_type = messages[0];

                    switch(msg_type){
                        case NEWJOIN:
                            msg_key = messages[1]; msg_value = messages[2];
                            hash_value = messages[3];
                            Log.v("ST", "In NEWJOIN");
                            String yourPred;
                            String yourSucc;
                            String you;
                            headpred = getPredecessor(head);
                            headsucc = getSuccessor(head);
                            if((hash_value.compareTo(head)>0) && (hash_value.compareTo(headsucc)<0))
                            {
                                Log.v("ST", "case 1");
                                yourPred = head;
                                yourSucc = headsucc;
                                you = hash_value;

                                String portOfSucc = getPortNumber(yourSucc);
                                String portOfYou = msg_key;
                                String portOfPred = getPortNumber(yourPred);
                                //send message to yourPred
                                String msgPred = UPDATE+myDelimiter+"P"+myDelimiter+NOTHING+myDelimiter+"S"+myDelimiter+you
                                                    +myDelimiter+"PP"+myDelimiter+NOTHING+myDelimiter+"SP"+myDelimiter+portOfYou;
                                if(portOfPred.compareTo(my_port_id)!=0){
                                    sendMsg(msgPred,portOfPred);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgPred);
                                }
                                else{
                                    succ_hash_id = you;
                                    succ_port_id = portOfYou;
                                }
                                //send message to you
                                String msgYou = UPDATE+myDelimiter+"P"+myDelimiter+yourPred+myDelimiter+"S"+myDelimiter+yourSucc
                                                +myDelimiter+"PP"+myDelimiter+portOfPred+myDelimiter+"SP"+myDelimiter+portOfSucc;
                                if(portOfYou.compareTo(my_port_id)!=0) {
                                    sendMsg(msgYou, portOfYou);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgYou);
                                }
                                else{
                                    pred_hash_id = yourPred;
                                    succ_hash_id = yourSucc;
                                    pred_port_id = portOfPred;
                                    succ_port_id = portOfSucc;
                                }
                                //send message to yourSucc
                                String msgSucc = UPDATE+myDelimiter+"P"+myDelimiter+you+myDelimiter+"S"+myDelimiter+NOTHING
                                                 +myDelimiter+"PP"+myDelimiter+portOfYou+myDelimiter+"SP"+myDelimiter+NOTHING;
                                if(portOfSucc.compareTo(my_port_id)!=0){
                                    sendMsg(msgSucc,portOfSucc);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgSucc);
                                }
                                else{
                                    pred_hash_id = you;
                                    pred_port_id = portOfYou;
                                }
                                chord_ring.put(you, msg_key);
                            }
                            else if((hash_value.compareTo(head)>0) && (hash_value.compareTo(headsucc)>0))
                            {
                                Log.v("ST", "case 2");
                                String theSucc = getSuc(hash_value, headsucc);
                                int position =  getNodePosition(theSucc);
                                String thePred;
                                if(position == 0)
                                {
                                    int size = chord_ring.size();
                                    if(size == 1)
                                    {
                                        thePred = head;
                                        theSucc = head;
                                    }
                                    else {
                                        thePred = (String) chord_ring.keySet().toArray()[size - 1];
                                    }
                                }
                                else{
                                    thePred = (String) chord_ring.keySet().toArray()[position-1];
                                }
                                yourPred = thePred;
                                yourSucc = theSucc;
                                you = hash_value;
                                //sending messages
                                String portOfSucc = getPortNumber(yourSucc);
                                String portOfYou = msg_key;
                                String portOfPred = getPortNumber(yourPred);
                                //send message to yourPred
                                String msgPred = UPDATE+myDelimiter+"P"+myDelimiter+NOTHING+myDelimiter+"S"+myDelimiter+you
                                        +myDelimiter+"PP"+myDelimiter+NOTHING+myDelimiter+"SP"+myDelimiter+portOfYou;
                                if(portOfPred.compareTo(my_port_id)!=0){
                                    sendMsg(msgPred,portOfPred);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgPred);
                                }
                                else{
                                    succ_hash_id = you;
                                    succ_port_id = portOfYou;
                                }
                                //send message to you
                                String msgYou = UPDATE+myDelimiter+"P"+myDelimiter+yourPred+myDelimiter+"S"+myDelimiter+yourSucc
                                        +myDelimiter+"PP"+myDelimiter+portOfPred+myDelimiter+"SP"+myDelimiter+portOfSucc;
                                if(portOfYou.compareTo(my_port_id)!=0) {
                                    sendMsg(msgYou, portOfYou);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgYou);
                                }
                                else{
                                    pred_hash_id = yourPred;
                                    succ_hash_id = yourSucc;
                                    pred_port_id = portOfPred;
                                    succ_port_id = portOfSucc;
                                }
                                //send message to yourSucc
                                String msgSucc = UPDATE+myDelimiter+"P"+myDelimiter+you+myDelimiter+"S"+myDelimiter+NOTHING
                                        +myDelimiter+"PP"+myDelimiter+portOfYou+myDelimiter+"SP"+myDelimiter+NOTHING;
                                if(portOfSucc.compareTo(my_port_id)!=0){
                                    sendMsg(msgSucc,portOfSucc);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgSucc);
                                }
                                else{
                                    pred_hash_id = you;
                                    pred_port_id = portOfYou;
                                }
                                chord_ring.put(you, msg_key);
                            }
                            else if((hash_value.compareTo(head)<0))
                            {
                                Log.v("ST", "case 3");
                                int size = chord_ring.size();
                                yourPred  = (String) chord_ring.keySet().toArray()[size - 1];
                                yourSucc  = head;
                                you = hash_value;
                                head = you;
                                //sending messages
                                String portOfSucc = getPortNumber(yourSucc);
                                String portOfYou = msg_key;
                                String portOfPred = getPortNumber(yourPred);
                                //send message to yourPred
                                String msgPred = UPDATE+myDelimiter+"P"+myDelimiter+NOTHING+myDelimiter+"S"+myDelimiter+you
                                        +myDelimiter+"PP"+myDelimiter+NOTHING+myDelimiter+"SP"+myDelimiter+portOfYou;
                                if(portOfPred.compareTo(my_port_id)!=0){
                                    sendMsg(msgPred,portOfPred);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgPred);
                                }
                                else{
                                    succ_hash_id = you;
                                    succ_port_id = portOfYou;
                                }
                                //send message to you
                                String msgYou = UPDATE+myDelimiter+"P"+myDelimiter+yourPred+myDelimiter+"S"+myDelimiter+yourSucc
                                        +myDelimiter+"PP"+myDelimiter+portOfPred+myDelimiter+"SP"+myDelimiter+portOfSucc;
                                if(portOfYou.compareTo(my_port_id)!=0) {
                                    sendMsg(msgYou, portOfYou);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgYou);
                                }
                                else{
                                    pred_hash_id = yourPred;
                                    succ_hash_id = yourSucc;
                                    pred_port_id = portOfPred;
                                    succ_port_id = portOfSucc;
                                }
                                //send message to yourSucc
                                String msgSucc = UPDATE+myDelimiter+"P"+myDelimiter+you+myDelimiter+"S"+myDelimiter+NOTHING
                                        +myDelimiter+"PP"+myDelimiter+portOfYou+myDelimiter+"SP"+myDelimiter+NOTHING;
                                if(portOfSucc.compareTo(my_port_id)!=0){
                                    sendMsg(msgSucc,portOfSucc);
//                                    PrintWriter pw = new PrintWriter(sock.getOutputStream(), true);
//                                    pw.println(msgSucc);
                                }
                                else{
                                    pred_hash_id = you;
                                    pred_port_id = portOfYou;
                                }
                                chord_ring.put(you, msg_key);
                            }
                            break;

                        case UPDATE:
                            Log.v("ST: Before "+my_port_id, "I am "+ my_node_id +" my Predecessor is "+ pred_hash_id +" my Successor is "+ succ_hash_id);
                            Log.v("CT", "Received pred and succ");
                            // msgType+myDelimiter+"P"+myDelimiter+""+myDelimiter+"S"+myDelimiter+you;
                            //  0                   1              2               3               4
                            //myDelimiter+"PP"+myDelimiter+""+myDelimiter+"SP"+myDelimiter+you
                            //             5                6               7               8
                            String myPred = messages[2];
                            Log.v("STU", "P: "+myPred);
                            String mySucc = messages[4];
                            Log.v("STU", "S: "+mySucc);
                            String myPredPort = messages[6];
                            Log.v("STU", "PP: "+myPredPort);
                            String mySuccPort = messages[8];
                            Log.v("STU", "SP: "+mySuccPort);

                            //if NOTHING is received don't update it.
                            if(myPred.compareTo(NOTHING)!=0)
                            {
                                pred_hash_id = myPred;
                            }
                            if(mySucc.compareTo(NOTHING)!=0)
                            {
                                succ_hash_id = mySucc;
                            }
                            if(myPredPort.compareTo(NOTHING)!=0)
                            {
                                pred_port_id = myPredPort;
                            }
                            if(mySuccPort.compareTo(NOTHING)!=0)
                            {
                                succ_port_id = mySuccPort;
                            }
                            Log.v("ST: After "+my_port_id, "I am "+ my_node_id +" my Predecessor is "+ pred_hash_id +" my Successor is "+ succ_hash_id);
                            break;

                        case INSERT:
                            String key_data1 = messages[1];
                            String value_data1 = messages[2];
                            ContentValues mycv = new ContentValues();
                            mycv.put("key", key_data1);
                            mycv.put("value", value_data1);
                            getContext().getContentResolver().insert(myUri, mycv);
                            break;

                        case QUERY:
                            String key = messages[1];
                            String sender = messages[2];
                            File file = new File(getContext().getFilesDir(),key);
                            if(file.exists())
                            {
                                String sb="";
                                try {
                                    int size;

                                    FileInputStream in_stream = getContext().openFileInput(key);
                                    while ((size = in_stream.read()) != -1) {
                                        sb += (char) size;
                                        //sb+= Character.toString((char) size);
                                    }
                                    Log.v("inside query 1","sb"+sb);
                                    in_stream.close();
                                } catch (FileNotFoundException e) {
                                    Log.e("inside else ","FNE");
                                } catch (IOException e) {
                                    Log.e("inside else ","IOE");
                                } catch (NullPointerException e) {
                                    Log.e("inside else ","NPE");
                                }
                                String message1 = QUERY_REPLY+myDelimiter+key+myDelimiter+sb+myDelimiter
                                                    +my_port_id;
                                sendMsg(message1, sender);
                            }
                            else
                            {
                                String message1 = QUERY+myDelimiter+key+myDelimiter+sender;
                                sendMsg(message1, succ_port_id);
                            }
                            break;

                        case QUERY_ALL:
                            String key1 = messages[1];
                            String sender1 = messages[2];
                            if(my_port_id.compareTo(sender1)==0)
                            {
                                foundOrigin = true;
                                break;
                            }
                            else
                            {
                                String mess = QUERY_ALL_REPLY+myDelimiter;
                                try{
                                    BufferedReader reader = null;
                                    String s = "";
                                    Log.v("0", "file reading");
                                    File file_path = new File(getContext().getFilesDir().getAbsolutePath());

                                    if (file_path.isDirectory()) {
                                        String[] filesy = file_path.list();
                                        int no_of_files = filesy.length;
                                        if (no_of_files > 0) {
                                            if (file_path.listFiles() != null) {
                                                for (File f : file_path.listFiles()) {
                                                    s = f.getName();
                                                    reader = new BufferedReader(new FileReader(file_path + "/" + s));
                                                    String value1 = reader.readLine();
                                                    String mshg = s + myDelimiter + value1 + myDelimiter;
                                                    mess = mess + mshg;
                                                }
                                                reader.close();

                                            }
                                        }
                                    }
                                    sendMsg(mess, sender1);
                                    String mess1 = QUERY_ALL+myDelimiter+key1+myDelimiter+sender1;
                                    sendMsg(mess1, succ_port_id);
                                    break;
                                } catch (Exception e) {
                                    Log.e("STAR_QUERY:ELSE ST", "file reading has failed"+e.getStackTrace());
                                }
                            }
                            break;

                        case QUERY_REPLY:
                            String key2 = messages[1];
                            String value2 = messages[2];
                            single_query.put(key2, value2);
                            break;

                        case QUERY_ALL_REPLY:
                            int len = messages.length;
                            if(len > 1)
                            {
                                int i = 1;
                                while(i<len)
                                {
                                    String key_1 = messages[i];
                                    String value_1 = messages[i+1];
                                    i = i+2;
                                    star_queries.put(key_1,value_1);
                                }
                            }
                            break;

                        case DELETE:
                            String sender2 = messages[2];
                            if(my_port_id.compareTo(sender2)==0)
                            {
                                Log.v("1", "file deleting");
                                deleteSingle = true;
                                break;
                            }
                            String del_key = messages[1];
                            File file1 = new File(getContext().getFilesDir(),del_key);
                            if(file1.exists())
                            {
                                if(file1.delete()){
                                Log.v("3", "file deleting");}
                            }
                            else
                            {
                                String message = DELETE+myDelimiter+del_key+myDelimiter+sender2;
                                sendMsg(message, succ_port_id);
                                Log.v("4", "file deleting");
                            }
                            break;

                        case DELETE_ALL:
                            String sender3 = messages[2];
                            if(my_port_id.compareTo(sender3)==0)
                            {
                                Log.v("5", "file deleting");
                                deleteAll= true;
                                break;
                            }
                            int count = 0;
                            String del_key1 = messages[1];
                            try {
                                BufferedReader reader = null;
                                File file_path = new File(getContext().getFilesDir().getAbsolutePath());
                                if (file_path.listFiles() != null) {
                                    for (File f : file_path.listFiles()) {
                                        if(f.delete())
                                        {
                                            count ++;
                                        }
                                    }
                                    Log.v("6", "file deleting");
                                    reader.close();
                                }
                            } catch (Exception e) {
                                Log.e("STAR_DELETE1", "file deleting has failed");
                            }
                            String message = DELETE_ALL+myDelimiter+del_key1+myDelimiter+sender3;
                            sendMsg(message, succ_port_id);
                            Log.v("8", "file deleting");
                            break;
                    }
                    printTreeMap();
                }

            }catch(IOException e){
                Log.e("SETask", "IOException ");
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {


        }
    }

}
