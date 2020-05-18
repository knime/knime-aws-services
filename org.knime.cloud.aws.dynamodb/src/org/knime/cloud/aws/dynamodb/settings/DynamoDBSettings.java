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
package org.knime.cloud.aws.dynamodb.settings;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import software.amazon.awssdk.regions.Region;

/**
 * Class for storing general DynamoDB settings.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBSettings extends AWSCredentialsSettings {

    private static final Region DEFAULT_REGION = Region.US_EAST_1;

    private static final String CFG_REGION = "region";
    private static final String CFG_ENDPOINT = "endpoint";

    private Region m_region = DEFAULT_REGION;
    private String m_endpoint = "";

    /**
     * @return the DynamoDB endpoint to use.
     * If empty, the default endpoint for the given region is used.
     */
    public String getEndpoint() {
        return m_endpoint;
    }
    
    /**
     * @param endpoint the DynamoDB endpoint to use.
     * If empty, the default endpoint for the given region is used.
     */
    public void setEndpoint(final String endpoint) {
        m_endpoint = endpoint;
    }

    /**
     * @return the region of the table as specified by Amazon
     * <a href="https://docs.aws.amazon.com/de_de/general/latest/gr/rande.html#ddb_region">here</a>.
     */
    public Region getRegion() {
        return m_region;
    }

    /**
     * Sets the region of the table.
     * @param region a region name as specified by Amazon
     * <a href="https://docs.aws.amazon.com/de_de/general/latest/gr/rande.html#ddb_region">here</a>.
     */
    public void setRegion(final Region region) {
        m_region = region;
    }

    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings) {
        super.saveSettings(settings);
        settings.addString(CFG_REGION, m_region.toString());
        settings.addString(CFG_ENDPOINT, m_endpoint);
    }

    /**
     * Loads settings from node settings.
     * @param settings the node settings to load from
     * @throws InvalidSettingsException if the settings cannot be loaded
     */
    @Override
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettings(settings);
        m_region = Region.of(settings.getString(CFG_REGION));
        m_endpoint = settings.getString(CFG_ENDPOINT);
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        super.loadSettingsForDialog(settings);
        m_region = Region.of(settings.getString(CFG_REGION, DEFAULT_REGION.toString()));
        m_endpoint = settings.getString(CFG_ENDPOINT, "");
    }
}
