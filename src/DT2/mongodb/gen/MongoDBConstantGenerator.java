package DT2.mongodb.gen;

import org.bson.Document;

import DT2.Randomly;
import DT2.common.ast.newast.Node;
import DT2.mongodb.MongoDBProvider.MongoDBGlobalState;
import DT2.mongodb.MongoDBSchema.MongoDBDataType;
import DT2.mongodb.ast.MongoDBConstant;
import DT2.mongodb.ast.MongoDBConstant.MongoDBBooleanConstant;
import DT2.mongodb.ast.MongoDBConstant.MongoDBDateTimeConstant;
import DT2.mongodb.ast.MongoDBConstant.MongoDBDoubleConstant;
import DT2.mongodb.ast.MongoDBConstant.MongoDBIntegerConstant;
import DT2.mongodb.ast.MongoDBConstant.MongoDBNullConstant;
import DT2.mongodb.ast.MongoDBConstant.MongoDBTimestampConstant;
import DT2.mongodb.ast.MongoDBExpression;

public class MongoDBConstantGenerator {
    private final MongoDBGlobalState globalState;

    public MongoDBConstantGenerator(MongoDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public Node<MongoDBExpression> generateConstantWithType(MongoDBDataType option) {
        switch (option) {
        case DATE_TIME:
            return MongoDBConstant.createDateTimeConstant(globalState.getRandomly().getInteger());
        case BOOLEAN:
            return MongoDBConstant.createBooleanConstant(Randomly.getBoolean());
        case DOUBLE:
            return MongoDBConstant.createDoubleConstant(globalState.getRandomly().getDouble());
        case STRING:
            return MongoDBConstant.createStringConstant(globalState.getRandomly().getString());
        case INTEGER:
            return MongoDBConstant.createIntegerConstant((int) globalState.getRandomly().getInteger());
        case TIMESTAMP:
            return MongoDBConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        default:
            throw new AssertionError(option);
        }
    }

    public void addRandomConstant(Document document, String key) {
        MongoDBDataType type = MongoDBDataType.getRandom(globalState);
        addRandomConstantWithType(document, key, type);
    }

    public void addRandomConstantWithType(Document document, String key, MongoDBDataType option) {
        MongoDBConstant constant;
        if (globalState.getDbmsSpecificOptions().testNullInserts && Randomly.getBooleanWithSmallProbability()) {
            constant = new MongoDBNullConstant();
            constant.setValueInDocument(document, key);
            return;
        }
        switch (option) {
        case DATE_TIME:
            constant = new MongoDBDateTimeConstant(globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;

        case BOOLEAN:
            constant = new MongoDBBooleanConstant(Randomly.getBoolean());
            constant.setValueInDocument(document, key);
            return;
        case DOUBLE:
            constant = new MongoDBDoubleConstant(globalState.getRandomly().getDouble());
            constant.setValueInDocument(document, key);
            return;
        case STRING:
            constant = new MongoDBConstant.MongoDBStringConstant(globalState.getRandomly().getString());
            constant.setValueInDocument(document, key);
            return;
        case INTEGER:
            constant = new MongoDBIntegerConstant((int) globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;
        case TIMESTAMP:
            constant = new MongoDBTimestampConstant(globalState.getRandomly().getInteger());
            constant.setValueInDocument(document, key);
            return;
        default:
            throw new AssertionError(option);
        }
    }
}
