package com.github.hackerwin7.mysql.tracker.mysql.driver.packets;

import com.github.hackerwin7.mysql.tracker.tracker.common.CanalToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

public abstract class CommandPacket implements IPacket {

    private byte command;

    // arg

    public void setCommand(byte command) {
        this.command = command;
    }

    public byte getCommand() {
        return command;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
