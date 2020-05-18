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
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.cloud.aws.dynamodb.createtable.IndexSettings;

import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

/**
 * A panel for describing an index.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public abstract class IndexPanel extends JPanel {

    private static final long serialVersionUID = 1L;
    
    /** All attributes. **/
    protected static final String PROJECTION_TYPE_ALL = "All attributes";
    /** Only key attributes. **/
    protected static final String PROJECTION_TYPE_KEYS_ONLY = "Only key attributes";
    /** Attributes given by the user. **/
    protected static final String PROJECTION_TYPE_INCLUDE = "User defined attributes";
    
    /**
     * Converts a ProjectionType to a human readable string.
     * @param t a DynamoDB ProjectionType
     * @return a human readable representation of the given projection type
     */
    protected static String projectionTypeToString(final ProjectionType t) {
        if (t == ProjectionType.ALL) {
            return PROJECTION_TYPE_ALL;
        } else if (t == ProjectionType.INCLUDE) {
            return PROJECTION_TYPE_INCLUDE;
        }
        return PROJECTION_TYPE_KEYS_ONLY;
    }
    
    /**
     * Converts a human readable string to a projection type.
     * @param type the string representation of the projection type.
     * @return a DynamoDB ProjectionType
     */
    protected static ProjectionType stringToProjectionType(final String type) {
        if (type.equals(PROJECTION_TYPE_ALL)) {
            return ProjectionType.ALL;
        } else if (type.equals(PROJECTION_TYPE_INCLUDE)) {
            return ProjectionType.INCLUDE;
        }
        return ProjectionType.KEYS_ONLY;
    }

    private List<RemoveListener> m_removeListeners = new ArrayList<>();
    
    /**
     * Creates a new instance of <code>IndexPanel</code>.
     */
    public IndexPanel() {
        setLayout(new GridBagLayout());
        setBorder(BorderFactory.createLineBorder(Color.DARK_GRAY));
    }
    
    /**
     * Creates an <code>IndexSettings</code> object from the information entered into the form.
     * @return <code>IndexSettings</code> corresponding to the input by the user
     */
    public abstract IndexSettings createIndexSettings();
    
    /**
     * Sets all input fields according to the passed settings.
     * @param idx the index settings to get the values from
     */
    public abstract void updateFromIndexSettings(final IndexSettings idx);
    
    @Override
    public Dimension getMaximumSize() {
        return new Dimension(super.getMaximumSize().width, getPreferredSize().height);
    }
    
    /**
     * Adds a new listener that listens for remove requests from this panel.
     * @param rl the listener to register
     */
    public void addRemoveListener(final RemoveListener rl) {
        m_removeListeners.add(rl);
    }
    
    /**
     * Removes a listener that listens for remove requests from this panel.
     * @param rl the listener to unregister
     */
    public void removeRemoveListener(final RemoveListener rl) {
        m_removeListeners.remove(rl);
    }
    
    /**
     * Let listeners know that panel should be removed.
     */
    protected void notifyRemoveListeners() {
        for (RemoveListener l : m_removeListeners) {
            l.removeRequested(this);
        }
    }
}
