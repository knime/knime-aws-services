/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   1 Jul 2019 (Alexander): created
 */
package org.knime.cloud.aws.dynamodb.utils;

import java.nio.charset.Charset;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.collection.SetDataValue;
import org.knime.core.data.json.JSONValue;
import org.knime.core.node.InvalidSettingsException;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Helper class for converting KNIME types to types for DynamoDB storage.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public final class KNIMEToDynamoDBUtil {

    private KNIMEToDynamoDBUtil() { }
    
    /**
     * AttributeValue representing a null value.
     */
    public static final AttributeValue NULL_ATTRIBUTE_VALUE = AttributeValue.builder().nul(true).build();
    
    /**
     * Creates a function for mapping KNIME data values to DynamoDB attribute values.
     * @param type the data type the mapper is for
     * @return a function mapping from KNIME data values to DynamoDB attribute values
     */
    public static Function<DataCell, AttributeValue> createMapper(final DataType type) {
        if (type.isCompatible(JSONValue.class)) {
            return v -> v.isMissing() ? NULL_ATTRIBUTE_VALUE : jsonToAttributeValue((JSONValue)v);
        } else if (type.isCompatible(StringValue.class)) {
            return v -> v.isMissing() ? NULL_ATTRIBUTE_VALUE : stringToAttributeValue((StringValue)v);
        } else if (type.isCompatible(BooleanValue.class)) {
            return v -> v.isMissing() ? NULL_ATTRIBUTE_VALUE : boolToAttributeValue((BooleanValue)v);
        } else if (type.isCompatible(DoubleValue.class)) {
            return v -> v.isMissing() ? NULL_ATTRIBUTE_VALUE : doubleToAttributeValue((DoubleValue)v);
        } else if (type.isCollectionType()) {
            return v -> v.isMissing() ? NULL_ATTRIBUTE_VALUE : collectionToListAttributeValue((CollectionDataValue)v);
        } else {
            return v -> dataCellToAttributeValue(v);
        }
    }
    
    /**
     * Creates mappers from KNIME cell to DynamoDB AttributeValues from a table spec.
     * @param spec the spec with the columns to map
     * @return an array of mapping functions
     * @throws InvalidSettingsException when no mapper is registered for a column type.
     */
    public static Function<DataCell, AttributeValue>[] createMappers(final DataTableSpec spec)
            throws InvalidSettingsException {
        @SuppressWarnings("unchecked")
        Function<DataCell, AttributeValue>[] mappers = new Function[spec.getNumColumns()];
        for (int i = 0; i < spec.getNumColumns(); i++) {
            DataColumnSpec cspec = spec.getColumnSpec(i);
            Function<DataCell, AttributeValue> mapper = KNIMEToDynamoDBUtil.createMapper(cspec.getType());
            mappers[i] = mapper;
        }
        return mappers;
    }
    
    /**
     * Creates an <code>AttributeValue</code> from a KNIME DataCell.
     * @param cell the cell to convert
     * @return a matching <code>AttributeValue</code>
     */
    public static AttributeValue dataCellToAttributeValue(final DataCell cell) {
        if (cell instanceof JSONValue) {
            return jsonToAttributeValue((JSONValue)cell);
        } else if (cell instanceof StringValue) {
            return stringToAttributeValue((StringValue)cell);
        } else if (cell instanceof BooleanValue) {
            return boolToAttributeValue((BooleanValue)cell);
        } else if (cell instanceof DoubleValue) {
            return doubleToAttributeValue((DoubleValue)cell);
        } else if (cell instanceof CollectionDataValue) {
            return collectionToListAttributeValue((CollectionDataValue)cell);
        } else {
            return unknownCellToAttributeValue(cell);
        }
    }
    
    /**
     * Converts a DataCell of unknown type to a string attribute value.
     * @param cell the cell to convert
     * @return a string attribute value containing the cell's content as string
     */
    public static AttributeValue unknownCellToAttributeValue(final DataCell cell) {
        return cell.isMissing() ? NULL_ATTRIBUTE_VALUE : AttributeValue.builder().s(cell.toString()).build();
    }
    
    /**
     * Converts a KNIME double value to a DynamoDB attribute value.
     * @param val the double value to convert
     * @return a matching attribute value
     */
    public static AttributeValue doubleToAttributeValue(final DoubleValue val) {
        return AttributeValue.builder().n(Double.toString(val.getDoubleValue())).build();
    }
    
    /**
     * Converts a KNIME string value to a DynamoDB attribute value.
     * @param val the string value to convert
     * @return a matching attribute value
     */
    public static AttributeValue stringToAttributeValue(final StringValue val) {
        return AttributeValue.builder().s(val.getStringValue()).build();
    }
    
    /**
     * Converts a KNIME json value to a DynamoDB attribute value.
     * @param val the json value to convert
     * @return a matching attribute value
     */
    public static AttributeValue jsonToAttributeValue(final JSONValue val) {
        return DynamoDBUtil.jsonValueToAttributeValue(val.getJsonValue());
    }
    
    /**
     * Converts a KNIME boolean value to a DynamoDB attribute value.
     * @param val the boolean value to convert
     * @return a matching attribute value
     */
    public static AttributeValue boolToAttributeValue(final BooleanValue val) {
        return AttributeValue.builder().bool(val.getBooleanValue()).build();
    }
    
    /**
     * Converts a KNIME collection to a DynamoDB list or set.
     * @param val the KNIME collection to convert
     * @return a matching DynamoDB list or set
     */
    public static AttributeValue collectionToListAttributeValue(final CollectionDataValue val) {
        if (val instanceof SetDataValue && val.getElementType().isCompatible(DoubleValue.class)) {
            return AttributeValue.builder()
                    .ns(val.stream().map((DataCell d) ->
                        Double.toString(((DoubleValue)d).getDoubleValue())).collect(Collectors.toList()))
                    .build();
        } else if (val instanceof SetDataValue && val.getElementType().isCompatible(StringValue.class)) {
            return AttributeValue.builder()
                    .ss(val.stream().map(s -> ((StringValue)s).getStringValue()).collect(Collectors.toList()))
                    .build();
        }
        Function<DataCell, AttributeValue> mapper = createMapper(val.getElementType());
        if (mapper == null) {
            mapper = v -> dataCellToAttributeValue(v);
        }
        return AttributeValue.builder()
                .l(val.stream().map(mapper).collect(Collectors.toList()))
                .build();
    }
    
    /**
     * Converts a data cell with a certain column spec to a DynamoDB {@link AttributeValue}.
     * @param cell the cell to convert
     * @param colSpec the column spec belonging to the cell
     * @param isBinary whether the cell, if it is a string cell, should be treated as base64 binary
     * @return an {@code AttributeValue} for the cell content
     */
    public static AttributeValue cellToKeyAttributeValue(final DataCell cell, final DataColumnSpec colSpec,
            final boolean isBinary) {
        if (colSpec.getType().isCompatible(StringValue.class)) {
            if (isBinary) {
                return AttributeValue.builder()
                        .b(SdkBytes.fromString(((StringValue)cell).getStringValue(), Charset.defaultCharset()))
                        .build();
                        
            } else {
                return AttributeValue.builder()
                        .s(((StringValue)cell).getStringValue())
                        .build();
            }
        } else {
            return AttributeValue.builder().n(Double.toString(((DoubleValue)cell).getDoubleValue())).build();
        }
    }
}