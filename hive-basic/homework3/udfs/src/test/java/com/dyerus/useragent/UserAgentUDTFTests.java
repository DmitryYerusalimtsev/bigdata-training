//package com.dyerus.useragent;
//
//import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
//
//import java.util.ArrayList;
//
//public class UserAgentUDTFTests {
//
//    @Test
//    public void testUserAgent() {
//
//        UserAgentUDTF udtf = new UserAgentUDTF();
//        StructObjectInspector inputOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector();
//        String name = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" " +
//                "\"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
//
//        try{
//            udtf.initialize(inputOI);
//        }catch(Exception ex){
//
//        }
//
//        ArrayList<Object[]> results = udtf.process(name);
//
//        Assert.assertEquals(1, results.size());
//        Assert.assertEquals("John", results.get(0)[0]);
//        Assert.assertEquals("Smith", results.get(0)[1]);
//    }
//}
