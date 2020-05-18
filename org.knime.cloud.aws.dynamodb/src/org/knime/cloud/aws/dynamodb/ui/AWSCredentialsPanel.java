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

import java.awt.CardLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import org.knime.cloud.aws.dynamodb.settings.AWSCredentialsSettings;
import org.knime.cloud.core.util.port.CloudConnectionInformation;

/**
 * A panel for entering AWS credentials.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class AWSCredentialsPanel extends ChangeObservablePanel {
    
    private static final String CREDENTIALS_PANEL = "credentials";
    private static final String OTHERS_PANEL = "others";
    
    private static final long serialVersionUID = 1L;
    
    /*
     * If this checkbox is unchecked, the user cannot enter credentials.
     * They will instead be retrieved from other sources, e.g. env variables.
     */
    private JCheckBox m_hasCredentials = new JCheckBox("Static credentials");
    private JTextField m_accessKey = new JTextField();
    private JPasswordField m_secretKey = new JPasswordField();
    private JLabel m_connectionInfo = new JLabel();
    
    /**
     * Creates a new instance of <code>AWSCredentialsPanel</code>.
     */
    public AWSCredentialsPanel() {
        setLayout(new CardLayout());
        JPanel card1 = createCredentialsPanel();
        JPanel card2 = createCredentialsSuppliedPanel();
        add(card1, CREDENTIALS_PANEL);
        add(card2, OTHERS_PANEL);
        
        setBorder(BorderFactory.createTitledBorder("Credentials"));
    }
    
    private JPanel createCredentialsPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;

        panel.add(m_hasCredentials);
        m_hasCredentials.addActionListener(e -> {
            m_accessKey.setEnabled(m_hasCredentials.isSelected());
            m_secretKey.setEnabled(m_hasCredentials.isSelected());
            notifyChangeListeners();
        });

        c.gridy++;
        panel.add(new JLabel("Access Key"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        panel.add(m_accessKey, c);
        m_accessKey.getDocument().addDocumentListener(new SingleMethodDocumentListener(this::notifyChangeListeners));

        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.gridx = 0;
        c.gridy++;
        panel.add(new JLabel("Secret Key"), c);
        c.gridx++;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        panel.add(m_secretKey, c);
        m_secretKey.getDocument().addDocumentListener(new SingleMethodDocumentListener(this::notifyChangeListeners));
        
        return panel;
    }
    
    private JPanel createCredentialsSuppliedPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 1;

        panel.add(new JLabel("<html><b>Credentials provided from input port:</b></html>"), c);
        
        c.gridy++;
        panel.add(m_connectionInfo, c);
        
        return panel;
    }
    
    /**
     * Sets the supplied connection from the input port or null if it is not connected.
     * @param con the connection information from the optional input port
     */
    public void setCloudConnectionInfo(final CloudConnectionInformation con) {
        CardLayout cl = (CardLayout)(this.getLayout());
        if (con == null) {
            cl.show(this, CREDENTIALS_PANEL);
        } else {
            cl.show(this, OTHERS_PANEL);
            m_connectionInfo.setText(String.format("Access key \"%s\" in region \"%s\"", con.getUser(), con.getHost()));
        }
    }
    
    /**
     * @return true if either "static credentials" is not checked or access and secret key are not empty.
     */
    public boolean hasValidCredentials() {
        return !m_hasCredentials.isSelected()
                || (m_accessKey.getText().trim().length() > 0 && m_secretKey.getPassword().length > 0);
    }
    
    /**
     * Changes all input fields at once.
     * @param settings the settings object to get the values for the inputs from
     */
    public void updateFromSettings(final AWSCredentialsSettings settings) {
        m_hasCredentials.setSelected(settings.isCredentialsGiven());
        m_accessKey.setText(settings.getAccessKey());
        m_secretKey.setText(settings.getSecretKey());
    }
    
    /**
     * Writes all credentials into a settings object.
     * @param settings the settings object to hold the credentials
     */
    public void saveToSettings(final AWSCredentialsSettings settings) {
        settings.setCredentialsGiven(m_hasCredentials.isSelected());
        settings.setAccessKey(m_accessKey.getText());
        settings.setSecretKey(new String(m_secretKey.getPassword()));
    }

    
    /**
     * @return true if the checkbox "static credentials" is  checked,
     * which means that the user can enter access and secret key
     */
    public boolean hasStaticCredentials() {
        return m_hasCredentials.isSelected();
    }
    
    /**
     * @return the access key for authentication with AWS
     */
    public String getAccessKey() {
        return m_accessKey.getText();
    }
    
    /**
     * @return the secret key for authentication with AWS
     */
    public String getSecretKey() {
        return new String(m_secretKey.getPassword());
    }
    
    /**
     * @param hasStatic true if the checkbox "static credentials" is  checked,
     * which means that the user can enter access and secret key
     */
    public void setHasStaticCredentials(final boolean hasStatic) {
        m_hasCredentials.setSelected(hasStatic);
    }
    
    /**
     * @param accessKey the access key for authentication with AWS
     */
    public void setAccessKey(final String accessKey) {
        m_accessKey.setText(accessKey);
    }
    
    /**
     * @param secretKey the secret key for authentication with AWS
     */
    public void setSecretKey(final String secretKey) {
        m_secretKey.setText(secretKey);
    }
}
