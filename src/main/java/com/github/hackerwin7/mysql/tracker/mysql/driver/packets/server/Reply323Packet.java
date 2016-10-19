package com.github.hackerwin7.mysql.tracker.mysql.driver.packets.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.github.hackerwin7.mysql.tracker.mysql.driver.packets.PacketWithHeaderPacket;
import com.github.hackerwin7.mysql.tracker.mysql.driver.utils.ByteHelper;

public class Reply323Packet extends PacketWithHeaderPacket {

    public byte[] seed;

    public void fromBytes(byte[] data) throws IOException {

    }

    public byte[] toBytes() throws IOException {
        if (seed == null) {
            return new byte[] { (byte) 0 };
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteHelper.writeNullTerminated(seed, out);
            return out.toByteArray();
        }
    }

}
