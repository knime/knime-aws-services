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
package org.knime.cloud.aws.dynamodb.ui.indexes;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableModel;

import org.knime.cloud.aws.dynamodb.createtable.IndexSettings;
import org.knime.cloud.aws.dynamodb.ui.AddEmptyRowTableModelListener;
import org.knime.cloud.aws.dynamodb.ui.EnumComboBox;
import org.knime.cloud.aws.dynamodb.utils.DynamoDBUtil;

import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.utils.StringUtils;

/**
 * A panel for describing an index.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class GlobalIndexPanel extends IndexPanel {

    private static final int MAX_CAPACITY_UNITS = 40000;
    private static final int DEFAULT_CAPACITY_UNITS = 5;
    
    private static final long serialVersionUID = 1L;
    private BillingMode m_billingMode = BillingMode.PROVISIONED;
    private JTextField m_name = new JTextField(10);
    private JTextField m_hashKeyName = new JTextField(10);
    private EnumComboBox<ScalarAttributeType> m_hashKeyType
        = new EnumComboBox<>(ScalarAttributeType.values(), DynamoDBUtil.getHumanReadableKeyTypes());
    private JCheckBox m_rangeKey = new JCheckBox("Range key");
    private JTextField m_rangeKeyName = new JTextField(10);
    private EnumComboBox<ScalarAttributeType> m_rangeKeyType
        = new EnumComboBox<>(ScalarAttributeType.values(), DynamoDBUtil.getHumanReadableKeyTypes());
    private JSpinner m_readUnits = new JSpinner(
            new SpinnerNumberModel(DEFAULT_CAPACITY_UNITS, 1, MAX_CAPACITY_UNITS, 1));
    private JSpinner m_writeUnits = new JSpinner(
            new SpinnerNumberModel(DEFAULT_CAPACITY_UNITS, 1, MAX_CAPACITY_UNITS, 1));
    
    private EnumComboBox<ProjectionType> m_projectionType = new EnumComboBox<>(ProjectionType.values(),
            new String[] {PROJECTION_TYPE_ALL, PROJECTION_TYPE_KEYS_ONLY, PROJECTION_TYPE_INCLUDE});
    
    private DefaultTableModel m_projAttrs = new DefaultTableModel(new String[] {"Non-Key Attributes"}, 1);
    private JScrollPane m_attrsScroller;
    
    /**
     * Creates a new instance of <code>IndexPanel</code>.
     */
    public GlobalIndexPanel() {
        setLayout(new GridBagLayout());
        setBorder(BorderFactory.createLineBorder(Color.DARK_GRAY));
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 1;
        c.anchor = GridBagConstraints.WEST;
        
        JLabel label = new JLabel("Global Index");
        Font f = label.getFont();
        label.setFont(f.deriveFont(f.getStyle() | Font.BOLD));
        
        add(label, c);
        
        c.gridx = 3;
        JButton removeBtn = new JButton("Remove");
        removeBtn.addActionListener(e -> notifyRemoveListeners());
        add(removeBtn, c);
        
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Name"), c);
        
        c.gridx++;
        add(m_name, c);
        
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Hash key"), c);
        
        c.gridx++;
        add(m_hashKeyName, c);
        
        c.gridx++;
        add(m_hashKeyType, c);
        
        c.gridx = 0;
        c.gridy++;
        m_rangeKey.addActionListener(e -> setRangeKeyEnabled(m_rangeKey.isSelected()));
        add(m_rangeKey, c);
        
        c.gridx++;
        add(m_rangeKeyName, c);
        
        c.gridx++;
        add(m_rangeKeyType, c);
        
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Read units"), c);
        
        c.gridx++;
        add(m_readUnits, c);
        
        c.gridx++;
        add(new JLabel("Write units"), c);
        
        c.gridx++;
        add(m_writeUnits, c);
        
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Projection"), c);
        
        c.gridx++;
        m_projectionType.addActionListener(e -> {
            m_attrsScroller.setVisible(m_projectionType.getSelectedItemValue() == ProjectionType.INCLUDE);
            validate();
            // We need to update the layout of the window
            Window w = SwingUtilities.getWindowAncestor(this);
            if (w != null) {
                w.pack();
            }
        });
        add(m_projectionType, c);
        
        c.gridx = 0;
        c.gridy++;
        m_projAttrs.addTableModelListener(new AddEmptyRowTableModelListener(m_projAttrs));
        JTable attrs = new JTable(m_projAttrs);
        attrs.setPreferredScrollableViewportSize(new Dimension(200, 100));
        attrs.setFillsViewportHeight(true);
        m_attrsScroller = new JScrollPane(attrs);
        m_attrsScroller.setMaximumSize(new Dimension(100, 100));
        c.gridwidth = 3;
        c.gridx = 1;
        m_attrsScroller.setVisible(false);
        add(m_attrsScroller, c);
    }

    /**
     * Sets the table's billing mode, controlling whether provisioned throughput can be entered or not.
     * @param mode the billing mode of the table
     */
    public void setBillingMode(final BillingMode mode) {
        m_billingMode = mode;
        setProvisionedThroughputEnabled(m_billingMode == BillingMode.PROVISIONED);
    }
    
    private void setProvisionedThroughputEnabled(final boolean enabled) {
        // Pay per request does not allow provisioned throughput
        boolean val = enabled && m_billingMode == BillingMode.PROVISIONED;
        m_readUnits.setEnabled(val);
        m_writeUnits.setEnabled(val);
    }
    
    private void setRangeKeyEnabled(final boolean enabled) {
        m_rangeKeyName.setEnabled(enabled);
        m_rangeKeyType.setEnabled(enabled);
    }
    
    /**
     * Creates an <code>IndexSettings</code> object from the information entered into the form.
     * @return <code>IndexSettings</code> corresponding to the input by the user
     */
    public IndexSettings createIndexSettings() {
        IndexSettings idx = new IndexSettings();
        idx.setType(IndexSettings.IndexType.GLOBAL);
        idx.setName(m_name.getText());
        idx.setHashKeyName(m_hashKeyName.getText());
        idx.setHashKeyType(m_hashKeyType.getSelectedItemValue());
        idx.setHasRangeKey(m_rangeKey.isSelected());
        
        idx.setRangeKeyName(m_rangeKeyName.getText());
        idx.setRangeKeyType(m_rangeKeyType.getSelectedItemValue());
        idx.setReadUnits((int)m_readUnits.getValue());
        idx.setWriteUnits((int)m_writeUnits.getValue());
        idx.setProjectionType(m_projectionType.getSelectedItemValue());
        idx.getProjection().clear();
        for (int i = 0; i < m_projAttrs.getRowCount(); i++) {
            String s = (String)m_projAttrs.getValueAt(i, 0);
            if (!StringUtils.isBlank(s)) {
                idx.getProjection().add(s);
            }
        }
        return idx;
    }
    
    /**
     * Sets all input fields according to the passed settings.
     * @param idx the index settings to get the values from
     */
    public void updateFromIndexSettings(final IndexSettings idx) {
        m_name.setText(idx.getName());
        m_hashKeyName.setText(idx.getHashKeyName());
        m_hashKeyType.setSelectedItemValue(idx.getHashKeyType());
        m_rangeKey.setSelected(idx.hasRangeKey());
        m_rangeKeyName.setText(idx.getRangeKeyName());
        m_rangeKeyType.setSelectedItemValue(idx.getRangeKeyType());
        m_readUnits.setValue(idx.getReadUnits());
        m_writeUnits.setValue(idx.getWriteUnits());
        m_projectionType.setSelectedItemValue(idx.getProjectionType());
        m_attrsScroller.setVisible(idx.getProjectionType() == ProjectionType.INCLUDE);
        setRangeKeyEnabled(idx.hasRangeKey());
        // One empty row
        m_projAttrs.setRowCount(0);
        m_projAttrs.setRowCount(1);
        // We go from the back and insert at 0 so the single empty row that is always there is in the right place
        for (int i = idx.getProjection().size() - 1; i >= 0; i--) {
            m_projAttrs.insertRow(0, new String[] {idx.getProjection().get(i)});
        }
    }
    
    @Override
    public Dimension getMaximumSize() {
        return new Dimension(super.getMaximumSize().width, getPreferredSize().height);
    }
}
