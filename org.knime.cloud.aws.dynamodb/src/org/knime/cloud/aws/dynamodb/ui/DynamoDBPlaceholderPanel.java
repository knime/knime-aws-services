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
package org.knime.cloud.aws.dynamodb.ui;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;

import org.knime.cloud.aws.dynamodb.ValueMapping;
import org.knime.cloud.aws.dynamodb.settings.DynamoDBPlaceholderSettings;
import org.knime.cloud.aws.dynamodb.utils.DynamoDBUtil;
import org.knime.core.data.DataTableSpec;

import software.amazon.awssdk.utils.StringUtils;

/**
 * Panel for specifying DynamoDB expression placeholders.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBPlaceholderPanel extends JPanel {

    private static final long serialVersionUID = 1L;
    
    /*
     * To use reserved keywords or column names with spaces etc.,
     * you have to put placeholders in their place and pass a mapping
     * from placeholder to name or value. Name placeholders must start with "#",
     * value placeholders with ":".
     * The user can enter the mapping in the following two tables.
     */
    private DefaultTableModel m_namesTblModel = new DefaultTableModel(0, 2);
    private DefaultTableModel m_valuesTblModel = new DefaultTableModel(0, 3);
    
    private String[] m_columnNames = new String[0];
    private boolean m_allowColumnValues;
    private JTable m_valuesTable;
    
    /**
     * Creates a new instance of {@code PlaceholderPanel} that does not allow column values as placeholder values.
     */
    public DynamoDBPlaceholderPanel() {
        this(false);
    }
    
    /**
     * Creates a new instance of {@code PlaceholderPanel}.
     * @param allowColumnValues whether placeholder values can be taken from a column
     */
    public DynamoDBPlaceholderPanel(final boolean allowColumnValues) {
        m_allowColumnValues = allowColumnValues;
        setLayout(new GridLayout(1, 2));
        GridBagConstraints tc = new GridBagConstraints();
        tc.gridx = 0;
        tc.gridy = 0;
        tc.fill = GridBagConstraints.HORIZONTAL;
        tc.weightx = 1;

        // Left panel: Names
        JPanel attrNames = new JPanel(new GridBagLayout());
        attrNames.add(new JLabel("Name Mapping"), tc);
        tc.gridy++;
        
        // Setup table for name placeholders
        JTable namesTable = new JTable();
        namesTable.setPreferredScrollableViewportSize(new Dimension(200, 200));
        m_namesTblModel.setColumnIdentifiers(new String[] {"Placeholder", "Name"});
        m_namesTblModel.addTableModelListener(new AddEmptyRowTableModelListener(m_namesTblModel));
        namesTable.setModel(m_namesTblModel);

        JScrollPane leftScrollPane = new JScrollPane(namesTable);
        leftScrollPane.setMaximumSize(new Dimension(100, 100));
        namesTable.setFillsViewportHeight(true);
        attrNames.add(leftScrollPane, tc);

        tc.gridy++;
        JPanel leftButtonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton removeNameRow = new JButton("-");
        // Let the user remove rows
        removeNameRow.addActionListener(e -> {
            ListSelectionModel sm = namesTable.getSelectionModel();
            int min = sm.getMinSelectionIndex();
            int max = sm.getMaxSelectionIndex();
            if (sm.getMinSelectionIndex() != -1) {
                for (int i = max; i >= min; i--) {
                    m_namesTblModel.removeRow(i);
                }
                if (m_namesTblModel.getRowCount() == 0) {
                    m_namesTblModel.setRowCount(1);
                }
            }
        });
        leftButtonPanel.add(removeNameRow);
        attrNames.add(leftButtonPanel, tc);

        // Right panel: Values
        tc.gridy = 0;
        JPanel attrValues = new JPanel(new GridBagLayout());
        attrValues.add(new JLabel("Value Mapping"), tc);
        tc.gridy++;

        // Setup table for value placeholders
        m_valuesTable = new JTable() {
            private static final long serialVersionUID = 1L;

            @Override
            public TableCellEditor getCellEditor(final int row, final int column) {
                if (column == 2 && this.getModel().getValueAt(row, 1).equals("COLUMN")) {
                    JComboBox<String> comboBox = new JComboBox<>(m_columnNames);
                    return new DefaultCellEditor(comboBox);
                }
                return super.getCellEditor(row, column);
            }
        };
        
        m_valuesTable.setPreferredScrollableViewportSize(new Dimension(200, 200));
        m_valuesTblModel.setColumnIdentifiers(new String[] {"Placeholder", "Type", "Value"});
        m_valuesTblModel.addTableModelListener(new AddEmptyRowTableModelListener(m_valuesTblModel, new int[] {0, 2}));
        m_valuesTable.setModel(m_valuesTblModel);
        
        // Second column contains a combo box for type selection
        String[] types = DynamoDBUtil.getTypes();
        if (allowColumnValues) {
            types = Arrays.copyOf(types, types.length + 1);
            types[types.length - 1] = "COLUMN";
        }
        JComboBox<String> typeBox = new JComboBox<>(types);
        m_valuesTable.getColumnModel().getColumn(1).setCellEditor(new DefaultCellEditor(typeBox));

        JScrollPane rightScrollPane = new JScrollPane(m_valuesTable);
        rightScrollPane.setMaximumSize(new Dimension(100, 100));
        m_valuesTable.setFillsViewportHeight(true);
        attrValues.add(rightScrollPane, tc);

        tc.gridy++;
        JPanel rightButtonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        JButton removeValueRow = new JButton("-");
        rightButtonPanel.add(removeValueRow);
        // Let the user remove rows
        removeValueRow.addActionListener(e -> {
            ListSelectionModel sm = m_valuesTable.getSelectionModel();
            int min = sm.getMinSelectionIndex();
            int max = sm.getMaxSelectionIndex();
            if (sm.getMinSelectionIndex() != -1) {
                for (int i = max; i >= min; i--) {
                    m_valuesTblModel.removeRow(i);
                }
                if (m_valuesTblModel.getRowCount() == 0) {
                    m_valuesTblModel.setRowCount(1);
                }
            }
        });
        attrValues.add(rightButtonPanel, tc);

        add(attrNames);
        add(attrValues);
    }
    
    /**
     * Updates the panel's fields with information from the settings.
     * Use this method only if the panel does not allow column values as placeholder values.
     * @param settings the settings to update the fields with
     */
    public void updateFromSettings(final DynamoDBPlaceholderSettings settings) {
        this.updateFromSettings(settings, null);
    }
    
    /**
     * Updates the panel's fields with information from the settings.
     * @param settings the settings to update the fields with
     * @param spec the table spec to select columns from or null if column values as placeholder values are not allowed.
     */
    public void updateFromSettings(final DynamoDBPlaceholderSettings settings, final DataTableSpec spec) {
        m_namesTblModel.setRowCount(0);
        for (Entry<String, String> e : settings.getNames().entrySet()) {
            String[] row = new String[] {e.getKey(), e.getValue()};
            if (m_namesTblModel.getRowCount() > 0) {
                m_namesTblModel.insertRow(m_namesTblModel.getRowCount() - 1, row);
            } else {
                m_namesTblModel.addRow(row);
            }
        }
        if (m_namesTblModel.getRowCount() == 0) {
            m_namesTblModel.setRowCount(1);
        }
        
        m_valuesTblModel.setRowCount(0);
        for (ValueMapping vm : settings.getValues()) {
            String[] row = new String[] {vm.getName(), vm.getType(), vm.getValue()};
            if (m_valuesTblModel.getRowCount() > 0) {
                m_valuesTblModel.insertRow(m_valuesTblModel.getRowCount() - 1, row);
            } else {
                m_valuesTblModel.addRow(row);
            }
        }
        if (m_valuesTblModel.getRowCount() == 0) {
            m_valuesTblModel.setRowCount(1);
        }
        
        if (m_allowColumnValues && spec != null) {
            m_columnNames = spec.getColumnNames();
            for (int i = 0; i < m_valuesTable.getRowCount(); i++) {
                if (m_valuesTable.getModel().getValueAt(i, 1) != null
                        && m_valuesTable.getModel().getValueAt(i, 1).equals("COLUMN")) {
                    DefaultCellEditor editor = (DefaultCellEditor)m_valuesTable.getCellEditor(i, 2);
                    @SuppressWarnings({ "unchecked", "rawtypes" })
                    JComboBox<String> cb = (JComboBox)editor.getComponent();
                    String value = (String)m_valuesTable.getModel().getValueAt(i, 2);
                    cb.removeAllItems();
                    for (String col : m_columnNames) {
                        cb.addItem(col);
                    }
                    cb.setSelectedItem(value);
                }
            }
        }
    }
    
    /**
     * Writes the values in this panel's fields into the settings.
     * @param settings the settings to update with the fields' values
     */
    public void saveToSettings(final DynamoDBPlaceholderSettings settings) {
        settings.getNames().clear();
        for (int i = 0; i < m_namesTblModel.getRowCount(); i++) {
            String name = (String)m_namesTblModel.getValueAt(i, 0);
            String value = (String)m_namesTblModel.getValueAt(i, 1);
            if (!StringUtils.isBlank(name) && !StringUtils.isBlank(value)) {
                settings.getNames().put(name, value);
            }
        }
        settings.getValues().clear();
        for (int i = 0; i < m_valuesTblModel.getRowCount(); i++) {
            String name = (String)m_valuesTblModel.getValueAt(i, 0);
            String type = (String)m_valuesTblModel.getValueAt(i, 1);
            String value = (String)m_valuesTblModel.getValueAt(i, 2);
            if (!StringUtils.isBlank(name) && !StringUtils.isBlank(type)
                    && !StringUtils.isBlank(value)) {
                settings.getValues().add(new ValueMapping(name, type, value));
            }
        }
    }

    /**
     * Removes all name placeholder mappings.
     */
    public void clearNameMappings() {
        m_namesTblModel.setRowCount(0);
    }
    
    /**
     * Removes all value placeholder mappings.
     */
    public void clearValueMappings() {
        m_valuesTblModel.setRowCount(0);
    }
    
    /**
     * Adds a row to the name placeholder mapping table.
     * @param placeholder the placeholder (#...)
     * @param name the attribute name to replace the placeholder with
     */
    public void addNameMapping(final String placeholder, final String name) {
        if (AddEmptyRowTableModelListener.isLastRowEmpty(m_namesTblModel, null)) {
            m_namesTblModel.insertRow(m_namesTblModel.getRowCount() - 1, new Object[] {placeholder, name});
        } else {
            m_namesTblModel.addRow(new Object[] {placeholder, name});
        }
    }
    
    /**
     * Adds a row to the value placeholder mapping table.
     * @param placeholder the placeholder (:...)
     * @param type the value type
     * @param value the value to replace the placeholder with
     */
    public void addValueMapping(final String placeholder, final String type, final String value) {
        if (AddEmptyRowTableModelListener.isLastRowEmpty(m_valuesTblModel, new int[] {0, 2})) {
            m_valuesTblModel.insertRow(m_valuesTblModel.getRowCount() - 1, new Object[] {placeholder, type, value});
        } else {
            m_valuesTblModel.addRow(new Object[] {placeholder, type, value});
        }
    }
    
    /**
     * @return the mapping from name placeholder to attribute name
     */
    public Map<String, String> getNameMappings() {
        Map<String, String> map = new HashMap<>();
        for (int row = 0; row < m_namesTblModel.getRowCount(); row++) {
            String name = (String) m_namesTblModel.getValueAt(row, 0);
            String mapping = (String) m_namesTblModel.getValueAt(row, 1);
            if (!StringUtils.isBlank(name) && !StringUtils.isBlank(mapping)) {
                map.put(name, mapping);
            }
        }
        return map;
    }
    
    /**
     * @return the mapping from value placeholder to attribute value
     */
    public List<ValueMapping> getValueMappings() {
        List<ValueMapping> map = new ArrayList<>();
        for (int row = 0; row < m_valuesTblModel.getRowCount(); row++) {
            String name = (String) m_valuesTblModel.getValueAt(row, 0);
            String type = (String) m_valuesTblModel.getValueAt(row, 1);
            String value = (String) m_valuesTblModel.getValueAt(row, 2);
            map.add(new ValueMapping(name, type, value));
        }
        return map;
    }
}
