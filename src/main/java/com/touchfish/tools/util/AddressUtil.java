package com.touchfish.tools.util;

import com.touchfish.tools.structure.IPFormat;
import redis.clients.jedis.HostAndPort;

public class AddressUtil {
    public static HostAndPort formatAddress(String address) {
        address = address.replace(" ", "").replace("[", "").replace("]", "");
        String host;
        String port;
        if (address.contains(":")){
            host = address.substring(0, address.lastIndexOf(":"));
            port = address.substring(address.lastIndexOf(":") + 1);
        } else {
            host = address;
            port = "";
        }
        return new HostAndPort(host, Integer.parseInt(port));
    }
    public static IPFormat getIPFormat(String address){
        if (address.contains("[") || address.chars().filter(c -> c == ':').count() > 1) {
            return IPFormat.IPV6;
        } else {
            return IPFormat.IPV4;
        }
    }
}
