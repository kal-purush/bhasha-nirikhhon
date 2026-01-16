package com.musala.sdcs.device.channel.base;

import com.musala.sdcs.device.channel.DimmingChannel;
import com.musala.sdcs.device.channel.SongControlChannel;
import com.musala.sdcs.device.channel.SongURLChannel;
import com.musala.sdcs.device.channel.TriggerChannel;
import com.musala.sdcs.device.channel.VolumeChannel;
import com.musala.sdcs.device.channel.type.PlayControlType;
import com.musala.sdcs.device.channel.type.TriggerType;

/**
 * Creates channel based on channel type given by the DB
 *
 */
public class ChannelCreator {
	
	
	/**
	 * @param id channel id
	 * @param label channel label
	 * @param command current command
	 * @param channelType channel type
	 * @return correct channel type
	 */
	public static Channel createChannel(Integer id, String label, String command, String channelType) {
		switch (channelType) {
		case "DimmingChannel":
			return new DimmingChannel(id, label, command);
		case "SongControlChannel":
			return new SongControlChannel(id, label, PlayControlType.getCommandValueFromString(command));
		case "SongURLChannel":
			return new SongURLChannel(id, label, command);
		case "TriggerChannel":
			return new TriggerChannel(id, label, TriggerType.getCommandValueFromString(command));
		case "VolumeChannel":
			return new VolumeChannel(id, label, command);
		}
		return null;
	}

}