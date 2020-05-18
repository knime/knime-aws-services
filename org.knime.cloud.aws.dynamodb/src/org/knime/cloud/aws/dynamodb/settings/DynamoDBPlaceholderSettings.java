package org.knime.cloud.aws.dynamodb.settings;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.cloud.aws.dynamodb.ValueMapping;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;

/**
 * Settings for placeholders in DynamoDB expressions.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBPlaceholderSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DynamoDBPlaceholderSettings.class);
    
    private static final String CFG_VALUES_NAME = "valName";
    private static final String CFG_VALUES_TYPE = "valType";
    private static final String CFG_VALUES_VALUES = "valValues";
    private static final String CFG_NAMES = "names";
    
    private List<ValueMapping> m_values = new ArrayList<>();
    private Map<String, String> m_names = new LinkedHashMap<String, String>();
    

    /**
     * @return a list of value placeholders for expressions
     */
    public List<ValueMapping> getValues() {
        return m_values;
    }

    /**
     * 
     * @param values a list of value placeholders for expressions
     */
    public void setValues(final List<ValueMapping> values) {
        m_values = values;
    }

    /**
     * 
     * @return a list of name placeholders for expressions
     */
    public Map<String, String> getNames() {
        return m_names;
    }

    /**
     * 
     * @param names a list of name placeholders for expressions
     */
    public void setNames(final Map<String, String> names) {
        m_names = names;
    }

    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    public void saveSettings(final NodeSettingsWO settings) {
        String[] vNames = new String[m_values.size()];
        String[] vTypes = new String[m_values.size()];
        String[] vValues = new String[m_values.size()];
        for (int i = 0; i < m_values.size(); i++) {
            ValueMapping vm = m_values.get(i);
            vNames[i] = vm.getName();
            vTypes[i] = vm.getType();
            vValues[i] = vm.getValue();
        }
        settings.addStringArray(CFG_VALUES_NAME, vNames);
        settings.addStringArray(CFG_VALUES_TYPE, vTypes);
        settings.addStringArray(CFG_VALUES_VALUES, vValues);
        
        Config names = settings.addConfig(CFG_NAMES);
        for (Entry<String, String> name : m_names.entrySet()) {
            names.addString(name.getKey(), name.getValue());
        }
    }

    /**
     * Loads settings from node settings.
     * @param settings the node settings to load from
     * @throws InvalidSettingsException if the settings cannot be loaded
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        String[] vNames = settings.getStringArray(CFG_VALUES_NAME);
        String[] vTypes = settings.getStringArray(CFG_VALUES_TYPE);
        String[] vValues = settings.getStringArray(CFG_VALUES_VALUES);
        
        m_values = new ArrayList<>();
        for (int i = 0; i < vNames.length; i++) {
            m_values.add(new ValueMapping(vNames[i], vTypes[i], vValues[i]));
        }
        
        m_names = new LinkedHashMap<String, String>();
        Config names = settings.getConfig(CFG_NAMES);
        for (String key : names.keySet()) {
            m_names.put(key, names.getString(key));
        }
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        String[] vNames = settings.getStringArray(CFG_VALUES_NAME, new String[0]);
        String[] vTypes = settings.getStringArray(CFG_VALUES_TYPE, new String[0]);
        String[] vValues = settings.getStringArray(CFG_VALUES_VALUES, new String[0]);
        
        m_values = new ArrayList<>();
        for (int i = 0; i < vNames.length; i++) {
            m_values.add(new ValueMapping(vNames[i], vTypes[i], vValues[i]));
        }
        
        m_names = new LinkedHashMap<String, String>();
        if (settings.containsKey(CFG_NAMES)) {
            try {
                Config names = settings.getConfig(CFG_NAMES);
                for (String key : names.keySet()) {
                    m_names.put(key, names.getString(key));
                }
            } catch (InvalidSettingsException e) {
                // Should not happen as we only query existing keys
                LOGGER.error(e);
            }
        }
    }
}
