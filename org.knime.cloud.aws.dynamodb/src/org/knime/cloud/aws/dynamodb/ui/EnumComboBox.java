package org.knime.cloud.aws.dynamodb.ui;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.swing.JComboBox;

/**
 * ComboBox displaying the values of an enum as human readable strings.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 * @param <E> the enum type this combo box is for
 */
public class EnumComboBox<E extends Enum<E>> extends JComboBox<String> {
    
    private static final long serialVersionUID = 1L;
    
    private E[] m_values;
    
    /**
     * Creates a new instance of this combo box for enums.
     * @param values the enum values to display
     * @param names the names assigned to the enum values
     */
    public EnumComboBox(final E[] values, final String[] names) {
        super(names);
        m_values = values;
    }
    
    /**
     * Creates a new instance of this combo box for enums using the enum item names as labels.
     * @param values the values of the enum
     */
    public EnumComboBox(final E[] values) {
        this(values, Arrays.stream(values).map(e -> e.toString()).collect(Collectors.toList()).toArray(new String[0]));
        m_values = values;
    }
    
    /**
     * Sets the selected item to the item corresponding to the passed enum value.
     * @param e the enum value to select the string label for
     */
    public void setSelectedItemValue(final E e) {
        setSelectedIndex(e.ordinal());
    }
    
    /**
     * Retrieves the name assigned to the selected item based on the array of strings
     * passed in the constructor.
     * @return the name matching the selected item
     */
    public E getSelectedItemValue() {
        if (getSelectedIndex() == -1) {
            return null;
        }
        return m_values[getSelectedIndex()];
    }
}
