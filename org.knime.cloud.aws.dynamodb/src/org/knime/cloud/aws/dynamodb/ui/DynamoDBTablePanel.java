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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.List;
import java.util.function.Supplier;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBTableSettings;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.FlowVariableModelButton;

import software.amazon.awssdk.regions.Region;

/**
 * A panel for entering DynamoDB table information (region, table name, endpoint).
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBTablePanel extends ChangeObservablePanel {
    
    private static final long serialVersionUID = 1L;
    
    private JComboBox<Region> m_region = new JComboBox<>(Region.regions().toArray(new Region[0]));
    private JTextField m_tableName = new JTextField();
    private JButton m_browseBtn;
    
    private JTextField m_endpoint = new JTextField();
    
    /**
     * Creates a new default instance of <code>DynamoDBTablePanel</code>.
     */
    public DynamoDBTablePanel() {
        this(null);
    }
    
    /**
     * Creates a new instance of <code>DynamoDBTablePanel</code>.
     * @param nameModel an optional flow variable model for the table name
     */
    public DynamoDBTablePanel(final FlowVariableModel nameModel) {
        this(nameModel, null);
    }
    
    /**
     * Creates a new instance of <code>DynamoDBTablePanel</code>.
     * @param nameModel an optional flow variable model for the table name
     * @param tableNameSupplier a supplier for a list of tables that is shown when clicking the browse button
     * or null if table browsing should not be enabled 
     */
    public DynamoDBTablePanel(final FlowVariableModel nameModel, final Supplier<List<String>> tableNameSupplier) {
        setLayout(new GridBagLayout());
        
        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;

        add(new JLabel("Region"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        add(m_region, c);
        m_region.addActionListener(e -> notifyChangeListeners());

        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Table Name"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        add(m_tableName, c);
        m_tableName.getDocument().addDocumentListener(new SingleMethodDocumentListener(this::notifyChangeListeners));
        
        if (tableNameSupplier != null) {
            c.gridx++;
            m_browseBtn = new JButton("Browse");
            m_browseBtn.addActionListener(e -> {
                List<String> tables = tableNameSupplier.get();
                if (tables == null) {
                    JOptionPane.showMessageDialog(null, "Could not load table list",
                            "Error", JOptionPane.ERROR_MESSAGE);
                } else if (tables.size() > 0) {
                    String table = (String) JOptionPane.showInputDialog(null, "Available tables for your account:",
                            "Select a table", JOptionPane.QUESTION_MESSAGE, null,
                            tables.toArray(new String[0]), tables.get(0));
                    m_tableName.setText(table);
                } else {
                    JOptionPane.showMessageDialog(null, "No tables available",
                            "Error", JOptionPane.ERROR_MESSAGE);
                }
            });
            add(m_browseBtn, c);
        }
        if (nameModel != null) {
            m_tableName.setEnabled(!nameModel.isVariableReplacementEnabled());
            FlowVariableModelButton nameFVButton = new FlowVariableModelButton(nameModel);
            nameModel.addChangeListener(new ChangeListener() {
                
                @Override
                public void stateChanged(final ChangeEvent e) {
                    m_tableName.setEnabled(!nameModel.isVariableReplacementEnabled());
                    if (m_browseBtn != null) {
                        m_browseBtn.setEnabled(!nameModel.isVariableReplacementEnabled());
                    }
                    if (nameModel.isVariableReplacementEnabled() && nameModel.getVariableValue().isPresent()) {
                        m_tableName.setText(nameModel.getVariableValue().get().getStringValue());
                    }
                }
            });
            
            c.gridx++;
            c.fill = GridBagConstraints.NONE;
            add(nameFVButton, c);
        }
        
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Custom Endpoint"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        add(m_endpoint, c);
        m_endpoint.getDocument().addDocumentListener(new SingleMethodDocumentListener(this::notifyChangeListeners));

        setBorder(BorderFactory.createTitledBorder("Table"));
    }
    
    /**
     * Sets a fixed region.
     * @param region the region to set or null if the user can choose
     */
    public void setRegionOverwrite(final Region region) {
        if (region != null) {
            m_region.setSelectedItem(region);
            m_region.setEnabled(false);
        } else {
            m_region.setEnabled(true);
        }
    }
    
    /**
     * Sets all input fields in this panel at once to the values stored in the given settings object.
     * @param settings the settings containing the table information
     */
    public void updateFromSettings(final DynamoDBTableSettings settings) {
        m_region.setSelectedItem(settings.getRegion());
        m_tableName.setText(settings.getTableName());
        m_endpoint.setText(settings.getEndpoint());
    }
    
    /**
     * Copies all information entered in this panel into a settings object.
     * @param settings the settings to copy the values from the panel into
     */
    public void saveToSettings(final DynamoDBTableSettings settings) {
        settings.setRegion((Region)m_region.getSelectedItem());
        settings.setTableName(m_tableName.getText());
        settings.setEndpoint(m_endpoint.getText());
    }
    
    /**
     * @return the AWS region of the table
     */
    public Region getRegion() {
        return (Region)m_region.getSelectedItem();
    }
    
    /**
     * @return the name of the DynamoDB table
     */
    public String getTableName() {
        return m_tableName.getText().trim();
    }
    
    /**
     * @return the endpoint of the table or empty if the default endpoint should be used
     */
    public String getEndpoint() {
        return m_endpoint.getText();
    }
    
    /**
     * @param r the region the table is in
     */
    public void setRegion(final Region r) {
        m_region.setSelectedItem(r);
    }
    
    /**
     * @param name the name of the table
     */
    public void setTableName(final String name) {
        m_tableName.setText(name);
    }
    
    /**
     * @param endpoint a custom endpoint or null or an empty string to use the default
     */
    public void setEndpoint(final String endpoint) {
        m_endpoint.setText(endpoint == null ? "" : endpoint);
    }
}
