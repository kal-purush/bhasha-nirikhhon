package model;

public enum KnowledgeType {
        //Enumeration  Literals
        STANDARIZATION("Standarization"),
        DOCUMENTATION("Documentation"),
        OPTIMIZATION("Optimization");
    
        //Attributes
        private final String description;
    
        //Methods
    
        //Extract list of descriptions
        public static String[] getKnowledgeTypes() {
            KnowledgeType[] types = KnowledgeType.values();
            String[] descriptions = new String[types.length];
            for (int i = 0; i < types.length; i++) {
                descriptions[i] = types[i].getDescription();
            }
            return descriptions;
        }
    
        //intToKnowledType
        public static KnowledgeType intToKnowledgeType(int intKnowledgeType){
            KnowledgeType knowledgeType= null;
    
            switch (intKnowledgeType) {
                case 1:
                    knowledgeType = STANDARIZATION;
                    break;
                case 2:
                    knowledgeType = DOCUMENTATION;
                    break;
                case 3:
                    knowledgeType = OPTIMIZATION;
                    break;
                default:
                    break;
            }
    
            return knowledgeType;
        }
    
    
        //CONSTRUCTOR
        private KnowledgeType(String description){
            this.description = description;
        }
    
    
        //GETTERS
        public String getDescription() {
            return description;
        }
}