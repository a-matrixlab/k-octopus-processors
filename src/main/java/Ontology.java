/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.v1.model.instance.Referenceable;

/**
 *
 * @author alexmy
 */
public class Ontology {

    public static void main(String[] args) {
        try {
            AtlasClient atlasClient = new AtlasClient(new String[]{"http://localhost:21000"}, new String[]{"admin", "admin"});

            Referenceable referenceable = new Referenceable("ontology");
            referenceable.set("name", "ontology");
            referenceable.set("description", "Root for all Buisness ontology categories.");
            referenceable.set("owner", "alexmy");
            referenceable.set("locationUri", "iot-metastore");

            String typeJSON = AtlasType.toV1Json(referenceable);
            System.out.println("Submitting new type= " + typeJSON + "\n\n");
            List<String> entitiesCreated = atlasClient.createType(typeJSON);

            System.out.println("Types registered with Atlas:");
            List<String> types = atlasClient.listTypes();
            types.forEach((type) -> {
                System.out.println("Type: " + type);
            });
            
        } catch (AtlasServiceException ex) {
            Logger.getLogger(Ontology.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
