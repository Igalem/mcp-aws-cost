import React, { useState, useRef, useEffect } from 'react';
import { Calendar } from 'lucide-react';

const DatePicker = ({ value, onChange, minDate, maxDate, id, className }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [selectedDate, setSelectedDate] = useState(value ? new Date(value) : null);
  const [currentMonth, setCurrentMonth] = useState(selectedDate || new Date());
  const datePickerRef = useRef(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (datePickerRef.current && !datePickerRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen]);

  // Update selectedDate when value prop changes
  useEffect(() => {
    if (value) {
      // Parse date string (YYYY-MM-DD) properly
      const dateParts = value.split('-');
      if (dateParts.length === 3) {
        const year = parseInt(dateParts[0], 10);
        const month = parseInt(dateParts[1], 10) - 1; // Month is 0-indexed
        const day = parseInt(dateParts[2], 10);
        const date = new Date(year, month, day);
        setSelectedDate(date);
        setCurrentMonth(date);
      } else {
        const date = new Date(value);
        if (!isNaN(date.getTime())) {
          setSelectedDate(date);
          setCurrentMonth(date);
        }
      }
    } else {
      setSelectedDate(null);
    }
  }, [value]);

  const parseDate = (dateStr) => {
    if (!dateStr) return null;
    const date = new Date(dateStr);
    return isNaN(date.getTime()) ? null : date;
  };

  const min = parseDate(minDate);
  const max = parseDate(maxDate);

  const getYears = () => {
    if (!min || !max) return [];
    const years = [];
    const startYear = min.getFullYear();
    const endYear = max.getFullYear();
    for (let year = startYear; year <= endYear; year++) {
      years.push(year);
    }
    return years;
  };

  const getMonths = () => {
    const months = [];
    const currentYear = currentMonth.getFullYear();
    
    for (let month = 0; month < 12; month++) {
      const firstDayOfMonth = new Date(currentYear, month, 1);
      const lastDayOfMonth = new Date(currentYear, month + 1, 0);
      
      // A month is valid if there's any overlap with the valid date range
      // Month is valid if: (first day <= max) AND (last day >= min)
      let isValid = true;
      
      if (min && lastDayOfMonth < min) {
        isValid = false;
      }
      if (max && firstDayOfMonth > max) {
        isValid = false;
      }
      
      if (isValid) {
        months.push(month);
      }
    }
    
    return months;
  };

  const getDaysInMonth = (year, month) => {
    return new Date(year, month + 1, 0).getDate();
  };

  const getFirstDayOfMonth = (year, month) => {
    return new Date(year, month, 1).getDay();
  };

  const isDateDisabled = (year, month, day) => {
    const date = new Date(year, month, day);
    // Compare dates by setting time to midnight and comparing date parts only
    const dateOnly = new Date(year, month, day);
    dateOnly.setHours(0, 0, 0, 0);
    
    if (min) {
      const minOnly = new Date(min);
      minOnly.setHours(0, 0, 0, 0);
      if (dateOnly < minOnly) return true;
    }
    if (max) {
      const maxOnly = new Date(max);
      maxOnly.setHours(0, 0, 0, 0);
      if (dateOnly > maxOnly) return true;
    }
    return false;
  };

  const handleDateSelect = (year, month, day) => {
    const date = new Date(year, month, day);
    if (isDateDisabled(year, month, day)) return;
    
    // Create date string in YYYY-MM-DD format
    const yearStr = year.toString();
    const monthStr = (month + 1).toString().padStart(2, '0');
    const dayStr = day.toString().padStart(2, '0');
    const dateStr = `${yearStr}-${monthStr}-${dayStr}`;
    
    setSelectedDate(date);
    onChange({ target: { value: dateStr } });
    setIsOpen(false);
  };

  const handleYearChange = (year) => {
    const newDate = new Date(currentMonth);
    newDate.setFullYear(year);
    
    // Check if current month is valid in the new year
    const firstDayOfMonth = new Date(year, newDate.getMonth(), 1);
    const lastDayOfMonth = new Date(year, newDate.getMonth() + 1, 0);
    
    let isValidMonth = true;
    if (min && lastDayOfMonth < min) isValidMonth = false;
    if (max && firstDayOfMonth > max) isValidMonth = false;
    
    // If current month is not valid, find the first valid month in the new year
    if (!isValidMonth) {
      const validMonths = [];
      for (let month = 0; month < 12; month++) {
        const firstDay = new Date(year, month, 1);
        const lastDay = new Date(year, month + 1, 0);
        let isValid = true;
        if (min && lastDay < min) isValid = false;
        if (max && firstDay > max) isValid = false;
        if (isValid) validMonths.push(month);
      }
      if (validMonths.length > 0) {
        newDate.setMonth(validMonths[0]);
      }
    }
    
    setCurrentMonth(newDate);
    
    // If selected date exists, update it to the new year
    if (selectedDate) {
      const updatedDate = new Date(selectedDate);
      updatedDate.setFullYear(year);
      // Clamp to valid range
      if (min && updatedDate < min) updatedDate.setTime(min.getTime());
      if (max && updatedDate > max) updatedDate.setTime(max.getTime());
      setSelectedDate(updatedDate);
    }
  };

  const handleMonthChange = (month) => {
    const newDate = new Date(currentMonth);
    newDate.setMonth(month);
    setCurrentMonth(newDate);
    
    // If selected date exists, update it to the new month
    if (selectedDate) {
      const updatedDate = new Date(selectedDate);
      updatedDate.setMonth(month);
      // Clamp to valid range
      if (min && updatedDate < min) updatedDate.setTime(min.getTime());
      if (max && updatedDate > max) updatedDate.setTime(max.getTime());
      setSelectedDate(updatedDate);
    }
  };

  const navigateMonth = (direction) => {
    const newDate = new Date(currentMonth);
    newDate.setMonth(newDate.getMonth() + direction);
    
    // Clamp to valid range
    if (min && newDate < min) {
      setCurrentMonth(new Date(min));
      return;
    }
    if (max) {
      const lastDayOfNewMonth = new Date(newDate.getFullYear(), newDate.getMonth() + 1, 0);
      if (lastDayOfNewMonth > max) {
        setCurrentMonth(new Date(max.getFullYear(), max.getMonth()));
        return;
      }
    }
    
    setCurrentMonth(newDate);
  };

  const displayValue = selectedDate 
    ? (() => {
        const year = selectedDate.getFullYear();
        const month = (selectedDate.getMonth() + 1).toString().padStart(2, '0');
        const day = selectedDate.getDate().toString().padStart(2, '0');
        return `${year}-${month}-${day}`;
      })()
    : (value || '');

  const years = getYears();
  const months = getMonths();
  const currentYear = currentMonth.getFullYear();
  const currentMonthIndex = currentMonth.getMonth();
  const daysInMonth = getDaysInMonth(currentYear, currentMonthIndex);
  const firstDay = getFirstDayOfMonth(currentYear, currentMonthIndex);

  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

  return (
    <div ref={datePickerRef} className="relative">
      <div className="relative">
        <input
          type="text"
          id={id}
          value={displayValue}
          readOnly
          onClick={() => setIsOpen(!isOpen)}
          className={className}
          placeholder="Select date"
        />
        <Calendar 
          className="absolute right-3 top-1/2 transform -translate-y-1/2 text-slate-400 pointer-events-none" 
          size={16} 
        />
      </div>
      
      {isOpen && (
        <div className="absolute z-50 mt-1 bg-white border border-gray-300 rounded-lg shadow-xl p-4 min-w-[280px]">
          {/* Year and Month Selectors */}
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <select
                value={currentYear}
                onChange={(e) => handleYearChange(parseInt(e.target.value))}
                className="bg-white border border-gray-300 rounded px-2 py-1 text-gray-900 text-sm focus:outline-none focus:border-blue-500"
              >
                {years.map(year => (
                  <option key={year} value={year}>{year}</option>
                ))}
              </select>
              <select
                value={currentMonthIndex}
                onChange={(e) => handleMonthChange(parseInt(e.target.value))}
                className="bg-white border border-gray-300 rounded px-2 py-1 text-gray-900 text-sm focus:outline-none focus:border-blue-500"
              >
                {months.map(month => (
                  <option key={month} value={month}>{monthNames[month]}</option>
                ))}
              </select>
            </div>
            <div className="flex gap-1">
              <button
                onClick={() => navigateMonth(-1)}
                className="bg-gray-100 hover:bg-gray-200 border border-gray-300 rounded px-2 py-1 text-gray-700 text-sm transition-colors"
                disabled={min && new Date(currentYear, currentMonthIndex - 1, 1) < min}
              >
                ‹
              </button>
              <button
                onClick={() => navigateMonth(1)}
                className="bg-gray-100 hover:bg-gray-200 border border-gray-300 rounded px-2 py-1 text-gray-700 text-sm transition-colors"
                disabled={max && new Date(currentYear, currentMonthIndex + 1, 0) > max}
              >
                ›
              </button>
            </div>
          </div>

          {/* Calendar Grid */}
          <div className="grid grid-cols-7 gap-1 mb-2">
            {dayNames.map(day => (
              <div key={day} className="text-center text-xs text-gray-600 font-medium py-1">
                {day}
              </div>
            ))}
          </div>
          
          <div className="grid grid-cols-7 gap-1">
            {/* Empty cells for days before the first day of the month */}
            {Array.from({ length: firstDay }).map((_, idx) => (
              <div key={`empty-${idx}`} className="h-8" />
            ))}
            
            {/* Days of the month */}
            {Array.from({ length: daysInMonth }).map((_, idx) => {
              const day = idx + 1;
              const isDisabled = isDateDisabled(currentYear, currentMonthIndex, day);
              const isSelected = selectedDate && 
                selectedDate.getFullYear() === currentYear &&
                selectedDate.getMonth() === currentMonthIndex &&
                selectedDate.getDate() === day;
              
              return (
                <button
                  key={day}
                  onClick={() => handleDateSelect(currentYear, currentMonthIndex, day)}
                  disabled={isDisabled}
                  className={`h-8 rounded text-sm transition-colors ${
                    isSelected
                      ? 'bg-blue-600 text-white font-semibold'
                      : isDisabled
                      ? 'text-gray-300 cursor-not-allowed'
                      : 'text-gray-700 hover:bg-gray-100'
                  }`}
                >
                  {day}
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default DatePicker;

