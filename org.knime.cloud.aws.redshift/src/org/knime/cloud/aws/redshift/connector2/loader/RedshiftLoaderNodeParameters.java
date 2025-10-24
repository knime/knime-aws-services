/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Oct 23, 2025 (Paul Bärnreuther): created
 */
package org.knime.cloud.aws.redshift.connector2.loader;

import java.util.List;
import java.util.function.Supplier;

import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.database.node.io.load.impl.fs.ConnectedLoaderParametersBase;
import org.knime.database.node.io.load.parameters.CSVFormatSettings;
import org.knime.database.node.io.load.parameters.CSVFormatSettings.CSVFormatSettingsModifier;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Inside;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;

/**
 * Web UI parameters for the Redshift Loader node.
 *
 * @author Paul Bärnreuther
 */
@SuppressWarnings("restriction")
@LoadDefaultsForAbsentFields
class RedshiftLoaderNodeParameters extends ConnectedLoaderParametersBase {

    @Widget(title = "Authorization parameters",
        description = "Such as IAM_ROLE 'arn:aws:iam::&lt;aws-account-id&gt;:role/&lt;role-name&gt;' or "
            + "ACCESS_KEY_ID '&lt;access-key-id&gt;'  SECRET_ACCESS_KEY '&lt;secret-access-key&gt;' "
            + "SESSION_TOKEN '&lt;temporary-token&gt;';")
    @Persist(configKey = "authorization")
    @Layout(AfterTargetFolder.class)
    String m_authorization = "";

    @Widget(title = "File Format", description = "The file format used to stage data before loading into Redshift.")
    @ValueReference(FileFormatRef.class)
    @Layout(AfterTargetFolder.class)
    RedshiftLoaderFileFormat m_fileFormatSelection = RedshiftLoaderFileFormat.PARQUET;

    static final class FileFormatRef implements ParameterReference<RedshiftLoaderFileFormat> {
    }

    static final class FileFormatIsCSV implements EffectPredicateProvider {

        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(FileFormatRef.class).isOneOf(RedshiftLoaderFileFormat.CSV);
        }
    }

    static final class FileFormatIsOrcOrParquet implements EffectPredicateProvider {

        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(FileFormatRef.class).isOneOf(//
                RedshiftLoaderFileFormat.ORC, //
                RedshiftLoaderFileFormat.PARQUET//
            );
        }
    }

    static final class RestrictCharacterEncodingAndExternalizeLayoutModification extends CSVFormatSettingsModifier {

        @Override
        protected Class<?> getExternalizedLayout() {
            return ExternalCSVFormatSection.class;
        }

        @Override
        protected Class<? extends StringChoicesProvider> getCharsetChoicesProvider() {
            return RedshiftCharacterSetChoicesProvider.class;
        }

    }

    static final class RedshiftCharacterSetChoicesProvider implements StringChoicesProvider {

        @Override
        public List<String> choices(final NodeParametersInput context) {
            return List.of("UTF-8", "UTF-16", "UTF-16BE", "UTF-16LE");
        }

    }

    @Widget(title = "Compression", description = "The compression method to use for staging files.")
    @ChoicesProvider(CompressionChoicesProvider.class)
    @Persist(configKey = "fileCompression")
    @Layout(AfterTargetFolder.class)
    String m_compression = "NONE";

    static final class CompressionChoicesProvider implements StringChoicesProvider {

        Supplier<RedshiftLoaderFileFormat> m_fileFormat;

        @Override
        public void init(final StateProviderInitializer initializer) {
            StringChoicesProvider.super.init(initializer);
            m_fileFormat = initializer.computeFromValueSupplier(FileFormatRef.class);
        }

        @Override
        public List<String> choices(final NodeParametersInput context) {
            RedshiftLoaderFileFormat format = m_fileFormat.get();
            return format.getCompressionFormats();
        }

    }

    static abstract class FileFormatToNumberProvider<T> implements StateProvider<T> {

        private Supplier<RedshiftLoaderFileFormat> m_fileFormatSupplier;

        abstract T getForFileFormat(RedshiftLoaderFileFormat format);

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fileFormatSupplier = initializer.computeFromValueSupplier(FileFormatRef.class);
        }

        @Override
        public T computeState(final NodeParametersInput parametersInput) throws StateComputationFailureException {
            final var format = m_fileFormatSupplier.get();
            return getForFileFormat(format);
        }

    }

    static final class WithinFileChunkSizeProvider extends FileFormatToNumberProvider<Integer> {

        @Override
        Integer getForFileFormat(final RedshiftLoaderFileFormat format) {
            return format.getDefaultChunkSize();
        }

    }

    static final class FileSizeProvider extends FileFormatToNumberProvider<Long> {

        @Override
        Long getForFileFormat(final RedshiftLoaderFileFormat format) {
            return format.getDefaultFileSize();
        }

    }

    @ValueProvider(WithinFileChunkSizeProvider.class)
    @Widget(title = "Chunk Size",
        description = "Within file chunk size for ORC/Parquet files. " + "<ul>"
            + "<li><b>ORC:</b> Within file Stripe size (rows)</li>"
            + "<li><b>Parquet:</b> Within file Row Group size (MB)</li>" + "</ul>")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Persist(configKey = "withinFileChunkSize")
    @Effect(predicate = FileFormatIsOrcOrParquet.class, type = EffectType.SHOW)
    @Layout(AfterTargetFolder.class)
    int m_chunkSize = RedshiftLoaderFileFormat.PARQUET.getDefaultChunkSize();

    @ValueProvider(FileSizeProvider.class)
    @Widget(title = "File Size",
        description = "Split data into files of size for ORC/Parquet files. " + "<ul>"
            + "<li><b>ORC:</b> Split data into files of size (rows)</li>"
            + "<li><b>Parquet:</b> Split data into files of size (MB)</li>" + "</ul>")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Persist(configKey = "fileSize")
    @Effect(predicate = FileFormatIsOrcOrParquet.class, type = EffectType.SHOW)
    @Layout(AfterTargetFolder.class)
    long m_fileSize = RedshiftLoaderFileFormat.PARQUET.getDefaultFileSize();

    @Effect(predicate = FileFormatIsCSV.class, type = EffectType.SHOW)
    @Advanced
    @Section(title = "CSV Format Settings")
    @Inside(AfterTargetFolder.class)
    interface ExternalCSVFormatSection {
    }

    @Modification(RestrictCharacterEncodingAndExternalizeLayoutModification.class)
    CSVFormatSettings m_fileFormat = new CSVFormatSettings();

}
