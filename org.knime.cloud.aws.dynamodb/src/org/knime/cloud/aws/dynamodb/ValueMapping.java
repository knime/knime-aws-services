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
package org.knime.cloud.aws.dynamodb;

import java.io.IOException;

import org.knime.cloud.aws.dynamodb.utils.DynamoDBUtil;
import org.knime.core.node.InvalidSettingsException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Class for storing the mapping from a placeholder name to a value of a certain type.
 * Used for expressions in DynamoDB, such as update, condition, or filter expressions.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class ValueMapping {
    
    /**
     * Special type for a placeholder that inserts a column value.
     */
    public static final String COLUMN_TYPE = "COLUMN";
    
    private String m_name;
    private String m_type;
    private String m_value;
    
    /**
     * Creates a new instance of <code>ValueMapping</code>.
     * @param name the placeholder name
     * @param type the type of the value for the placeholder
     * @param value the value to replace the placeholder with
     */
    public ValueMapping(final String name, final String type, final String value) {
        m_name = name;
        m_type = type;
        m_value = value;
    }

    /**
     * @return the placeholder name, must start with a colon (":")
     */
    public String getName() {
        return m_name;
    }
    
    /**
     * @param name the placeholder name, must start with a colon (":")
     */
    public void setName(final String name) {
        m_name = name;
    }
    
    /**
     * @return the type of value for this placeholder
     */
    public String getType() {
        return m_type;
    }
    
    /**
     * @param type the type of value for this placeholder (S, SS, B, BS, N, NS, M, NUL, BOOL, COLUMN)
     */
    public void setType(final String type) {
        m_type = type;
    }
    
    /**
     * @return string representation of the value to insert
     */
    public String getValue() {
        return m_value;
    }
    
    /**
     * Sets the value to replace the placeholder with.
     * @param value string representation of the value to insert
     */
    public void setValue(final String value) {
        m_value = value;
    }
    
    /**
     * Creates a DynamoDB AttributeValue from the value and type stored in this instance.
     * @return an AttributeValue for the given string value for usage in DynamoDB.
     * @throws InvalidSettingsException when the given value cannot be parsed
     */
    public AttributeValue getAttributeValue() throws InvalidSettingsException {
        if (m_type.equals(COLUMN_TYPE)) {
            throw new IllegalStateException("Column values cannot be converted to an AttributeValue");
        }
        ObjectMapper om = new ObjectMapper();
        JsonNode node;
        try {
            if (m_type.equals(DynamoDBUtil.BINARY_TYPE) || m_type.equals(DynamoDBUtil.STRING_TYPE)) {
                node = om.readTree(String.format("\"%s\"", m_value));
            } else {
                node = om.readTree(m_value);
            }
        } catch (IOException e) {
            throw new InvalidSettingsException("Cannot parse the given value as JSON: " + m_value, e);
        }
        return DynamoDBUtil.toAttribute(m_type, node);
    }

    /**
     * @return true if the type is COLUMN
     */
    public boolean isColumnType() {
        return m_type.equals(COLUMN_TYPE);
    }
}
