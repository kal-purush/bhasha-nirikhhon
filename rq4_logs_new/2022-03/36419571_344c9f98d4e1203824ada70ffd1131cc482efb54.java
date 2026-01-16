/**
 * Copyright (c) 2014 Center for eHalsa i samverkan (CeHis).
 * 								<http://cehis.se/>
 *
 * This file is part of SKLTP.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package se.skltp.admin.contributors;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class developerInfo implements InfoContributor {

    private static List<String> splitToList(String separatedString) {
        List<String> list = Arrays.asList(separatedString.split("[.]"));
        return list;
    }

    private static LinkedHashMap<String, Object> propToTree(Properties props) {
        LinkedHashMap<String, Object> sys = new LinkedHashMap<String, Object>();
        for (Object key : props.keySet()) {
            LinkedHashMap<String, Object> node = sys;
            String lastSub = null;
            LinkedHashMap<String, Object> pn = null;
            for (String sub : splitToList((String) key)) {
                Object subNode = node.get(sub);
                pn = node;
                lastSub = sub;
                if (subNode instanceof Map) {
                    node = (LinkedHashMap<String, Object>) subNode;
                } else {
                    node.put(sub, new LinkedHashMap<String, Object>());
                    node = (LinkedHashMap<String, Object>) node.get(sub);
                }
            }
            pn.put(lastSub, props.get(key));
        }
        return sys;
    }

    @Override
    public void contribute(Info.Builder builder) {
        builder.withDetail("system", propToTree(System.getProperties()));
        builder.withDetail("environment", System.getenv());
    }
}