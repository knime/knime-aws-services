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

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBKeyColumnsSettings;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.ColumnFilter;
import org.knime.core.node.util.ColumnSelectionPanel;

/**
 * A panel for selecting DynamoDB  hash and range key columns in a KNIME table.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBKeyColumnsPanel extends JPanel {
    
    private static final long serialVersionUID = 1L;

    // Filter for hash and range key columns
    private static final ColumnFilter FILTER = new ColumnFilter() {
        
        @Override
        public boolean includeColumn(final DataColumnSpec colSpec) {
            return colSpec.getType().isCompatible(StringValue.class)
                    || colSpec.getType().isCompatible(DoubleValue.class);
        }
        
        @Override
        public String allFilteredMsg() {
            return "No string or number column found";
        }
    };
    
    private ColumnSelectionPanel m_hashColumn = new ColumnSelectionPanel(
            BorderFactory.createEmptyBorder(0, 0, 0, 0), FILTER, false, false);
    private ColumnSelectionPanel m_rangeColumn = new ColumnSelectionPanel(
            BorderFactory.createEmptyBorder(0, 0, 0, 0), FILTER, true, false);
    private JCheckBox m_hashKeyBinary = new JCheckBox("as Base64 binary");
    private JCheckBox m_rangeKeyBinary = new JCheckBox("as Base64 binary");
    
    /**
     * Creates a new instance of {@code DynamoDBKeyColumnsPanel}.
     */
    public DynamoDBKeyColumnsPanel() {
        setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 1;
        c.anchor = GridBagConstraints.WEST;
        
        add(new JLabel("Hash Key Column"), c);
        c.gridx++;
        m_hashColumn.setRequired(true);
        m_hashColumn.addActionListener(e -> {
            m_hashKeyBinary.setEnabled(mayBeBinaryColumn(m_hashColumn.getSelectedColumnAsSpec()));
        });
        add(m_hashColumn, c);
        
        c.gridx++;
        add(m_hashKeyBinary, c);
        
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Range Key Column"), c);
        c.gridx++;
        m_rangeColumn.setRequired(false);
        m_rangeColumn.addActionListener(e -> {
            m_rangeKeyBinary.setEnabled(mayBeBinaryColumn(m_rangeColumn.getSelectedColumnAsSpec()));
        });
        add(m_rangeColumn, c);
        
        c.gridx++;
        add(m_rangeKeyBinary, c);
    }
    
    private boolean mayBeBinaryColumn(final DataColumnSpec colSpec) {
        return colSpec != null && !colSpec.getType().equals(DataType.getMissingCell().getType())
                && colSpec.getType().isCompatible(StringValue.class);
    }
    
    /**
     * Updates the fields in this panel with values from settings.
     * @param inSpec the spec of the input table
     * @param settings the settings to update from
     * @throws NotConfigurableException If the spec does not contain at least one compatible type.
     */
    public void updateFromSettings(final DataTableSpec inSpec, final DynamoDBKeyColumnsSettings settings)
            throws NotConfigurableException {
        m_hashColumn.update(inSpec, settings.getHashKeyColumn());
        m_hashKeyBinary.setSelected(settings.isHashKeyBinary());
        
        m_rangeColumn.update(inSpec, settings.getRangeKeyColumn());
        m_rangeKeyBinary.setSelected(settings.isRangeKeyBinary());
        m_hashKeyBinary.setEnabled(mayBeBinaryColumn(m_hashColumn.getSelectedColumnAsSpec()));
        m_rangeKeyBinary.setEnabled(mayBeBinaryColumn(m_rangeColumn.getSelectedColumnAsSpec()));
    }
    
    /**
     * Fills a settings object with the values entered in this panel by the user.
     * @param settings the settings to write to
     */
    public void saveToSettings(final DynamoDBKeyColumnsSettings settings) {
        settings.setHashKeyColumn(m_hashColumn.getSelectedColumn());
        settings.setHashKeyBinary(m_hashKeyBinary.isSelected());
        settings.setRangeKeyColumn(m_rangeColumn.getSelectedColumn());
        settings.setRangeKeyBinary(m_rangeKeyBinary.isSelected());
    }
}
