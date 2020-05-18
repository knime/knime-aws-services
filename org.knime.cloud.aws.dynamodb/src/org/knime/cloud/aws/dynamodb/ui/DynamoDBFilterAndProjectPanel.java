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

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBPlaceholderSettings;

/**
 * A panel with controls for entering filter and project expressions for DynamoDB.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBFilterAndProjectPanel extends JPanel {

    private static final long serialVersionUID = 1L;
    
    private JTextField m_filter = new JTextField();
    private JTextField m_projection = new JTextField();
    
    private DynamoDBPlaceholderPanel m_placeholders = new DynamoDBPlaceholderPanel();
    
    /**
     * Creates a new instance of <code>DynamoDBFilterAndProjectPanel</code>.
     */
    public DynamoDBFilterAndProjectPanel() {
        setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 0;
        add(new JLabel("Filter"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        add(m_filter, c);

        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.gridx = 0;
        c.gridy++;
        add(new JLabel("Projection"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        add(m_projection, c);

        c.gridy++;
        c.gridx = 0;
        c.fill = GridBagConstraints.BOTH;
        c.gridwidth = 2;
        add(m_placeholders, c);
    }
    
    /**
     * @return the expression for filtering values returned by DynamoDB
     */
    public String getFilterExpression() {
        return m_filter.getText();
    }
    
    /**
     * @param expr the expression for filtering values returned by DynamoDB
     */
    public void setFilterExpression(final String expr) {
        m_filter.setText(expr);
    }
    
    /**
     * @return the expression for choosing attributes to return from DynamoDB
     */
    public String getProjectionExpression() {
        return m_projection.getText();
    }
    
    /**
     * @param expr the expression for choosing attributes to return from DynamoDB
     */
    public void setProjectionExpression(final String expr) {
        m_projection.setText(expr);
    }
    
    /**
     * Loads the values in this panel's placeholder fields from the settings.
     * @param settings the settings to update with the fields' values from
     */
    public void updatePlaceholdersFromSettings(final DynamoDBPlaceholderSettings settings) {
        m_placeholders.updateFromSettings(settings);
    }
    
    /**
     * Writes the values in this panel's placeholder fields into the settings.
     * @param settings the settings to update with the fields' values
     */
    public void savePlaceholdersToSettings(final DynamoDBPlaceholderSettings settings) {
        m_placeholders.saveToSettings(settings);
    }
}
