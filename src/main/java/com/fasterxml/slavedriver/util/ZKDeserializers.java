package com.fasterxml.slavedriver.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.slavedriver.NodeInfo;
import com.fasterxml.slavedriver.NodeState;

/**
 * Helper classes needed to work with "ZooKeeperMap"
 */
public class ZKDeserializers
{
   /**
    * Utility method for converting an array of bytes to a NodeInfo object.
    */
   public static class NodeInfoDeserializer implements Function<byte[], NodeInfo>
   {
       final protected Logger LOG = LoggerFactory.getLogger(getClass());

       @Override
       public NodeInfo apply(byte[] bytes) {
           try {
               return JsonUtil.fromJSON(bytes, NodeInfo.class);
           } catch (Exception e) {
               String str = Strings.stringFromUtf8(bytes);
               NodeInfo info = new NodeInfo(NodeState.Shutdown.toString(), 0);
               LOG.warn("Saw node data in non-JSON format. Have to default to {}. Input = {}", info, str);
               return info;
           }
       }
   }

   /**
    * Utility method for converting an array of bytes to a String.
    */
   public static class StringDeserializer implements Function<byte[], String> {
       @Override
       public String apply(byte[] a) {
           try {
               return Strings.stringFromUtf8(a);
           } catch (Exception e) { // can this ever occur?
               return "";
           }
     }
   }

   public static class ObjectNodeDeserializer implements Function<byte[], ObjectNode> {
       final protected Logger LOG = LoggerFactory.getLogger(getClass());

       @Override
       public ObjectNode  apply(byte[] input)
       {
           if (input != null && input.length > 0) {
               try {
                   return JsonUtil.fromJSON(input);
               } catch (Exception e) {
                   LOG.error("Failed to de-serialize ZNode", e);
               }
           }
           return JsonUtil.objectNode();
     }
   }

   /**
    * Utility method for converting an array of bytes to a Double.
    */
   public static class DoubleDeserializer implements Function<byte[], Double>
   {
       @Override
       public Double apply(byte[] a) {
           try {
               return new Double(Strings.stringFromUtf8(a));
           } catch (Exception e) {
               return 0d;
           }
       }
   }
}
