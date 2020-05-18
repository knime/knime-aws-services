package org.knime.cloud.aws.dynamodb.ui;

import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableModel;

import software.amazon.awssdk.utils.StringUtils;

/**
 * Listener that ensures that a {@link DefaultTableModel} always ends with an empty row.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class AddEmptyRowTableModelListener implements TableModelListener {

    private DefaultTableModel m_model;
    private int[] m_checkedCols;

    /**
     * Creates a new instance of a <code>AddEmptyRowTableModelListener</code> listener that only
     * checks the passed column indices for emptiness. 
     * @param model the model to observe
     * @param checkedCols the columns to check
     */
    public AddEmptyRowTableModelListener(final DefaultTableModel model, final int[] checkedCols) {
        m_model = model;
        m_checkedCols = checkedCols;
    }

    /**
     * Creates a new instance of a <code>AddEmptyRowTableModelListener</code> listener.
     * @param model the model to observe
     */
    public AddEmptyRowTableModelListener(final DefaultTableModel model) {
        this(model, null);
    }

    @Override
    public void tableChanged(final TableModelEvent e) {
        if (m_model.getRowCount() > 0) {
            if (!isLastRowEmpty(m_model, m_checkedCols)) {
                m_model.setRowCount(m_model.getRowCount() + 1);
            }
        }
    }
    
    /**
     * Checks if the last row of the table model is empty.
     * @param model the model to check the last row of
     * @param checkedCols the indices of the columns to check or null if all columns should be checked
     * @return true if the last row in the table model is empty
     */
    public static boolean isLastRowEmpty(final DefaultTableModel model, final int[] checkedCols) {
        if (model.getRowCount() == 0) {
            return false;
        }
        if (checkedCols != null) {
            for (int idx : checkedCols) {
                String value = (String) model.getValueAt(model.getRowCount() - 1, idx);
                if (!StringUtils.isBlank(value)) {
                    return false;
                }
            }
        } else {
            for (int idx = 0; idx < model.getColumnCount(); idx++) {
                String value = (String) model.getValueAt(model.getRowCount() - 1, idx);
                if (!StringUtils.isBlank(value)) {
                    return false;
                }
            }
        }
        return true;
    }
}
