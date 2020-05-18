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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.List;
import java.util.function.Supplier;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

/**
 * A panel for selecting an index.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class IndexSelectionPanel extends JPanel {

    private static final long serialVersionUID = 1L;
    
    private JCheckBox m_useIndex = new JCheckBox("Use Index");
    private JTextField m_indexName = new JTextField(10);
    private JButton m_browseBtn;
    
    /**
     * Creates a new instance of {@code IndexSelectionPanel}.
     * @param indexNameSupplier a supplier function for table names.
     * Invoked when the user presses the Browse-button.
     */
    public IndexSelectionPanel(final Supplier<List<String>> indexNameSupplier) {
        setLayout(new GridBagLayout());
        GridBagConstraints kc = new GridBagConstraints();
        kc.insets = new Insets(2, 2, 2, 2);
        kc.gridx = 0;
        kc.gridy = 0;
        kc.weightx = 1;
        kc.fill = GridBagConstraints.HORIZONTAL;
        
        m_useIndex.addActionListener(e -> {
            m_indexName.setEnabled(m_useIndex.isSelected());
            if (m_browseBtn != null) {
                m_browseBtn.setEnabled(m_useIndex.isSelected());
            }
        });
        add(m_useIndex, kc);
        
        kc.gridx++;
        add(m_indexName, kc);
        
        if (indexNameSupplier != null) {
            kc.gridx++;
            kc.weightx = 0;

            m_browseBtn = new JButton("Browse");
            m_browseBtn.addActionListener(e -> {
                List<String> indexes = indexNameSupplier.get();
                if (indexes == null) {
                    JOptionPane.showMessageDialog(null, "Could not load index list",
                            "Error", JOptionPane.ERROR_MESSAGE);
                } else if (indexes.size() > 0) {
                    String index = (String) JOptionPane.showInputDialog(null, "Available indexes for the table:",
                            "Select an index", JOptionPane.QUESTION_MESSAGE, null,
                            indexes.toArray(new String[0]), indexes.get(0));
                    m_indexName.setText(index);
                } else {
                    JOptionPane.showMessageDialog(null, "No indexes available for the given table",
                            "Error", JOptionPane.ERROR_MESSAGE);
                }
            });
            add(m_browseBtn, kc);
        }
    }
    
    /**
     * Updates the fields in this component with the given name.
     * @param indexName the index name or null if no index should be used
     * @param useIndex true if the checkbox should be checked and an index used
     */
    public void update(final String indexName, final boolean useIndex) {
        if (!useIndex) {
            m_indexName.setText("");
            m_useIndex.setSelected(false);
        } else {
            m_indexName.setText(indexName);
            m_useIndex.setSelected(true);
        }
        m_indexName.setEnabled(useIndex);
        if (m_browseBtn != null) {
            m_browseBtn.setEnabled(useIndex);
        }
    }
    
    /**
     * @return the name of the index to use
     */
    public String getIndexName() {
        return m_indexName.getText();
    }
    
    /**
     * @return true if an index should be used
     */
    public boolean isUseIndex() {
        return m_useIndex.isSelected();
    }
}
