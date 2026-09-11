package com.etchv;
import com.google.gson.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
public class AssetsTest {
 @Test void assetProtocol() throws Exception {
  var record=JsonParser.parseString(Files.readString(Path.of("tests/assets.json"))).getAsJsonObject();String id=record.get("id").getAsString();
  var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
  var errors=new ArrayList<Throwable>();
  server.createContext("/assets",exchange->{
   int status=200;String output=record.toString();
   try{
    assertEquals("test-key",exchange.getRequestHeaders().getFirst("X-API-Key"));
    String method=exchange.getRequestMethod(),query=Objects.toString(exchange.getRequestURI().getRawQuery(),""),path=exchange.getRequestURI().getPath();
    if(query.contains("cursor=")){status=409;output="{}";}
    else if(method.equals("PATCH")){var body=JsonParser.parseString(new String(exchange.getRequestBody().readAllBytes(),StandardCharsets.UTF_8)).getAsJsonObject();assertEquals(1,body.get("version").getAsInt());assertEquals("renamed",body.get("name").getAsString());var updated=record.deepCopy();updated.addProperty("version",2);output=updated.toString();}
    else if(method.equals("DELETE")){status=204;}
    else if(method.equals("POST")){var body=JsonParser.parseString(new String(exchange.getRequestBody().readAllBytes(),StandardCharsets.UTF_8)).getAsJsonObject();assertEquals(id,body.getAsJsonArray("asset_ids").get(0).getAsString());status=204;}
    else if(path.endsWith("/content")){output="file";}
    else if(path.equals("/assets")){assertTrue(query.contains("kind=watermarked"));output="{\"items\":["+record+"],\"next_cursor\":\"next-page\"}";}
   }catch(Throwable e){errors.add(e);status=500;output="{}";}
   byte[] bytes=output.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(status,status==204?-1:bytes.length);if(status!=204)exchange.getResponseBody().write(bytes);exchange.close();
  });server.start();
  try(var c=new EtchvClient("test-key","http://127.0.0.1:"+server.getAddress().getPort(),Duration.ofSeconds(2))){
   assertEquals("next-page",c.listAssets(Map.of("kind","watermarked")).nextCursor());
   assertEquals("launch",c.getAsset(id).metadata().get("campaign").getAsString());
   assertEquals(2,c.updateAsset(id,1,Map.of("name","renamed")).version());
   assertArrayEquals("file".getBytes(StandardCharsets.UTF_8),c.downloadAsset(id));c.deleteAsset(id);c.deleteAssets(List.of(id));
   assertEquals(409,assertThrows(EtchvClient.EtchvException.class,()->c.listAssets(Map.of("cursor","next-page"))).statusCode);
   assertThrows(IllegalArgumentException.class,()->c.getAsset("../other"));assertTrue(errors.isEmpty(),errors.toString());
  }finally{server.stop(0);}
 }
}
