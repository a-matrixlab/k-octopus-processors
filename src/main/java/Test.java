/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Arrays;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.v1.model.typedef.ClassTypeDefinition;
import org.apache.atlas.v1.model.typedef.TypesDef;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.v1.model.typedef.TraitTypeDefinition;
import org.apache.atlas.v1.model.typedef.EnumTypeDefinition;
import org.apache.atlas.v1.model.typedef.StructTypeDefinition;
import org.apache.atlas.v1.model.typedef.AttributeDefinition;
import org.apache.atlas.v1.model.typedef.Multiplicity;

/**
 *
 * @author alexmy
 */
public class Test {

    static final String DATABASE_TYPE = "DB_v11";
    static final String ONTOLOGY_TYPE = "ontology";
    static final String D_ONTOLOGY_TYPE = "d_ontology";
    static final String CATALOG_TYPE = "catalog";
    static final String D_CATALOG_TYPE = "d_catalog";

    private static final String[] TYPES
            = {DATABASE_TYPE, ONTOLOGY_TYPE, D_ONTOLOGY_TYPE, //CATALOG_TYPE, D_CATALOG_TYPE, "JdbcAccess_v11"
        };
    private static AtlasClient atlasClient;

    public Test() {
        atlasClient = new AtlasClient(new String[]{"http://localhost:21000"}, new String[]{"admin", "admin"});
    }

    public static void main(String[] args) {

        Test test = new Test();
        try {
            test.run();
        } catch (Exception ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }
//        System.out.println("Types registered with Atlas:");
//        try {
//            List<String> types = atlasClient.listTypes();
//            System.out.println("Types registered with Atlas:\n" + types);
//        } catch (AtlasServiceException ex) {
//            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        System.out.println("Types registered with Atlas:");
//        try {
//            List<String> types = atlasClient.listTypes();
//            System.out.println("Types registered with Atlas:\n" + types);
//        } catch (AtlasServiceException ex) {
//            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    private void run() throws Exception {
        listTypes();
        createTypes();
        verifyTypesCreated();
        createEntity();

    }

    private void verifyTypesCreated() throws AtlasServiceException {
        List<String> types = atlasClient.listTypes();
        for (String type : TYPES) {
            assert types.contains(type);
        }
    }

    private void listTypes() throws AtlasServiceException {
        System.out.println("Types registered with Atlas:");
        List<String> types = atlasClient.listTypes();
        types.forEach((type) -> {
            System.out.println("Type: " + type);
        });
    }

    private void createTypes() throws Exception {
        TypesDef typesDef = createTypeDefinitions();

        String typesAsJSON = AtlasType.toV1Json(typesDef);
        System.out.println("typesAsJSON = " + typesAsJSON);
        atlasClient.createType(typesAsJSON);

        // verify types created
        listTypes();
    }

    TypesDef createTypeDefinitions() throws Exception {
        ClassTypeDefinition ontologyDef = TypesUtil
                .createClassTypeDef(
                        D_ONTOLOGY_TYPE, // name
                        D_ONTOLOGY_TYPE, // description
                        null, // Set<String> - set of superTypes
                        TypesUtil.createUniqueRequiredAttrDef("name", AtlasBaseTypeDef.ATLAS_TYPE_STRING),
                        attrDef("description", AtlasBaseTypeDef.ATLAS_TYPE_STRING),
                        attrDef("locationUri", AtlasBaseTypeDef.ATLAS_TYPE_STRING),
                        attrDef("owner", AtlasBaseTypeDef.ATLAS_TYPE_STRING),
                        attrDef("createTime", AtlasBaseTypeDef.ATLAS_TYPE_LONG));

//        TraitTypeDefinition jdbcTraitDef = TypesUtil.createTraitTypeDef("JdbcAccess_v11", "JdbcAccess Trait", null);
        return new TypesDef(Collections.<EnumTypeDefinition>emptyList(), Collections.<StructTypeDefinition>emptyList(),
                null, //Arrays.asList(jdbcTraitDef),
                Arrays.asList(ontologyDef));
    }

    AttributeDefinition attrDef(String name, String dT) {
        return attrDef(name, dT, Multiplicity.OPTIONAL, false, null);
    }

    AttributeDefinition attrDef(String name, String dT, Multiplicity m, boolean isComposite,
            String reverseAttributeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dT);
        return new AttributeDefinition(name, dT, m, isComposite, reverseAttributeName);
    }

    private String createEntity()
            throws AtlasServiceException {

        Referenceable referenceable = new Referenceable("ontology");
        referenceable.set("name", "beer_ontology");
        referenceable.set("description", "desc");
        referenceable.set("owner", "alexmy");
        referenceable.set("locationUri", "test-schema-v2");

        String entityJSON = AtlasType.toV1Json(referenceable);
        System.out.println("Submitting new entity= " + entityJSON);
        List<String> entitiesCreated = atlasClient.createEntity(entityJSON);

        System.out.println("created instance for type " + "avro" + ", guid: " + entitiesCreated);
        entitiesCreated.forEach((entity) -> {
            System.out.println("Entity created: " + entity);
        });
        return entitiesCreated.get(entitiesCreated.size() - 1);
    }
}
