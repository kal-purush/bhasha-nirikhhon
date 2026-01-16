/*
 * Copyright (c) 2016 Villu Ruusmann
 *
 * This file is part of JPMML-Piglet
 *
 * JPMML-Piglet is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Piglet is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Piglet.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.piglet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import org.apache.pig.PigException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.OutputField;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.OutputUtil;
import org.jpmml.evaluator.PMMLException;
import org.jpmml.evaluator.TypeAnalysisException;

public class EvaluatorUtil {

	private EvaluatorUtil(){
	}

	/**
	 * <p>
	 * Instantiates a mapper function from PMML field names to Apache Pig tuple field indices.
	 * </p>
	 *
	 * @throws PigException If there is no valid mapping.
	 */
	static
	public Function<FieldName, Integer> inputSchemaMapper(Evaluator evaluator, Schema inputSchema) throws PigException {
		Map<String, Integer> aliasIndices = new LinkedHashMap<>();

		List<Schema.FieldSchema> fieldSchemas = inputSchema.getFields();
		for(int i = 0; i < fieldSchemas.size(); i++){
			Schema.FieldSchema fieldSchema = fieldSchemas.get(i);

			aliasIndices.put((fieldSchema.alias).toLowerCase(), Integer.valueOf(i));
		}

		final
		Map<FieldName, Integer> activeFieldIndices = new LinkedHashMap<>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			DataField dataField = evaluator.getDataField(activeField);

			DataType dataType = dataField.getDataType();

			Integer aliasIndex = aliasIndices.get((activeField.getValue()).toLowerCase());
			if(aliasIndex == null){
				throw new PigException("Field " + activeField + " not defined");
			}

			FieldSchema fieldSchema = fieldSchemas.get(aliasIndex);

			DataType fieldDataType = DataTypeUtil.parseDataType(fieldSchema.type);

			if(!DataTypeUtil.isCompatible(dataType, fieldDataType)){
				throw new PigException("Field " + activeField + " does not support " + fieldDataType + " data. Must be " + dataType + " data");
			}

			activeFieldIndices.put(activeField, aliasIndex);
		}

		Function<FieldName, Integer> result = new Function<FieldName, Integer>(){

			@Override
			public Integer apply(FieldName activeField){
				return activeFieldIndices.get(activeField);
			}
		};

		return result;
	}

	/**
	 * @throw PigException If the Apache Pig layer malfunctions.
	 * It is likely that all input tuples are affected, and the execution should be aborted.
	 * @throw PMMLException If the PMML layer malfunctions.
	 * It is likely that only the current tuple is affected, and the execution could proceed with other tuples.
	 */
	static
	public Tuple exec(Evaluator evaluator, Function<FieldName, Integer> activeFieldMapper, Tuple input) throws PigException, PMMLException {
		Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		for(FieldName activeField : activeFields){
			Integer activeFieldIndex = activeFieldMapper.apply(activeField);
			if(activeFieldIndex == null){
				throw new PigException("Field " + activeField + " not defined");
			}

			FieldValue activeValue = org.jpmml.evaluator.EvaluatorUtil.prepare(evaluator, activeField, input.get(activeFieldIndex));

			arguments.put(activeField, activeValue);
		}

		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		List<Object> resultValues = new ArrayList<>();

		List<FieldName> targetFields = evaluator.getTargetFields();
		for(FieldName targetField : targetFields){
			resultValues.add(org.jpmml.evaluator.EvaluatorUtil.decode(result.get(targetField)));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			resultValues.add(result.get(outputField));
		}

		return EvaluatorUtil.tupleFactory.newTuple(resultValues);
	}

	static
	public Schema outputSchema(Evaluator evaluator, Schema inputSchema){
		List<Schema.FieldSchema> tupleFieldSchemas = new ArrayList<>();

		List<FieldName> targetFields = evaluator.getTargetFields();
		for(FieldName targetField : targetFields){
			DataField dataField = evaluator.getDataField(targetField);

			org.dmg.pmml.DataType dataType = dataField.getDataType();

			tupleFieldSchemas.add(new Schema.FieldSchema(targetField.getValue(), DataTypeUtil.formatDataType(dataType)));
		}

		List<FieldName> outputFields = evaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			OutputField output = evaluator.getOutputField(outputField);

			org.dmg.pmml.DataType dataType = output.getDataType();
			if(dataType == null){

				try {
					dataType = OutputUtil.getDataType(output, (ModelEvaluator<?>)evaluator);
				} catch(TypeAnalysisException tae){
					dataType = org.dmg.pmml.DataType.STRING;
				}
			}

			tupleFieldSchemas.add(new Schema.FieldSchema(outputField.getValue(), DataTypeUtil.formatDataType(dataType)));
		}

		FieldSchema tupleSchema;

		try {
			tupleSchema = new FieldSchema(null, new Schema(tupleFieldSchemas), org.apache.pig.data.DataType.TUPLE);
		} catch(FrontendException fe){
			throw new RuntimeException(fe);
		}

		return new Schema(tupleSchema);
	}

	private static final TupleFactory tupleFactory = TupleFactory.getInstance();
}