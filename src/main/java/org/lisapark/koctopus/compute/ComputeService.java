/* 
 * Copyright (C) 2019 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.lisapark.koctopus.compute;

import java.util.Properties;
import static spark.Spark.*;

/**
 * Here I am using very simple and light spark micro web framework.
 * You can easily replace it with Spring boot or whatever.
 * 
 * Also I am using a default port = 4567.
 * 
 * http://localhost:4567/dhbs/[command]
 * 
 * Parameters go into Request body.
 * 
 * @author alexmylnikov
 */
public class ComputeService {

    Properties props;
    
    public static void main(String[] args) {
        // Set Server port
        int _port = 4567;
        
        if(args.length > 0){
            _port = Integer.valueOf(args[0]);
        }
         port(_port);
        
        // Map requests
        post("/dhbs/update-hbase", (req, res) -> { return Controller.process(req, res, "update-hbase"); });
        post("/dhbs/matched-pairs", (req, res) -> { return Controller.process(req, res, "matched-pairs"); });
        post("/dhbs/discover-pks", (req, res) -> { return Controller.process(req, res, "discover-pks"); });
        post("/dhbs/flush-redis", (req, res) -> { return Controller.process(req, res, "flush-redis"); });
        post("/dhbs/restore-redis", (req, res) -> { return Controller.process(req, res, "restore-redis"); });
        post("/dhbs/group-builder", (req, res) -> { return Controller.process(req, res, "group-builder"); });
    }    
}
