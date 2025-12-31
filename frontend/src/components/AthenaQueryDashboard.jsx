import React, { useState, useEffect } from 'react';
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { MessageSquare, Database, TrendingUp, Users, Calendar, Send, X, Filter } from 'lucide-react';

const AthenaQueryDashboard = () => {
  const [queries, setQueries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [chatOpen, setChatOpen] = useState(false);
  const [chatMessages, setChatMessages] = useState([]);
  const [inputMessage, setInputMessage] = useState('');
  const [chatLoading, setChatLoading] = useState(false);
  const [selectedWorkgroup, setSelectedWorkgroup] = useState('all');
  const [selectedWorkgroupLine, setSelectedWorkgroupLine] = useState(null); // For filtering workgroup lines
  
  // Date range filtering
  const getDefaultDateRange = () => {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 13); // Last 14 days default
    return {
      startDate: startDate.toISOString().split('T')[0],
      endDate: endDate.toISOString().split('T')[0]
    };
  };
  
  const [dateRange, setDateRange] = useState(getDefaultDateRange());

  // Fetch data from backend API
  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch('http://localhost:8000/api/dashboard/stats');
        if (response.ok) {
          const data = await response.json();
          setQueries(data.queries || []);
        } else {
          // Fallback to sample data if API fails
          const sampleData = generateSampleData();
          setQueries(sampleData);
        }
      } catch (error) {
        console.error('Error fetching data:', error);
        // Fallback to sample data
        const sampleData = generateSampleData();
        setQueries(sampleData);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const generateSampleData = () => {
    const workgroups = ['analytics', 'data-science', 'reporting', 'adhoc'];
    const dates = [];
    for (let i = 29; i >= 0; i--) {
      const date = new Date();
      date.setDate(date.getDate() - i);
      dates.push(date.toISOString().split('T')[0]);
    }

    return dates.flatMap(date => 
      workgroups.map(workgroup => ({
        date,
        workgroup,
        query_count: Math.floor(Math.random() * 50) + 10,
        scanned_size_mb: Math.floor(Math.random() * 5000) + 500,
        avg_execution_time: Math.floor(Math.random() * 30) + 5
      }))
    );
  };

  // Filter queries by selected workgroup and date range
  const getFilteredQueries = () => {
    let filtered = queries;
    
    // Filter by workgroup
    if (selectedWorkgroup !== 'all') {
      filtered = filtered.filter(q => q.workgroup === selectedWorkgroup);
    }
    
    // Filter by date range
    if (dateRange.startDate && dateRange.endDate) {
      filtered = filtered.filter(q => {
        const queryDate = new Date(q.date);
        const startDate = new Date(dateRange.startDate);
        const endDate = new Date(dateRange.endDate);
        endDate.setHours(23, 59, 59, 999); // Include the entire end date
        return queryDate >= startDate && queryDate <= endDate;
      });
    }
    
    return filtered;
  };
  
  // Quick date range selectors
  const setQuickDateRange = (range) => {
    const endDate = new Date();
    const startDate = new Date();
    
    switch (range) {
      case '1D':
        startDate.setDate(startDate.getDate() - 1);
        break;
      case '1W':
        startDate.setDate(startDate.getDate() - 7);
        break;
      case '1M':
        startDate.setMonth(startDate.getMonth() - 1);
        break;
      default:
        return;
    }
    
    setDateRange({
      startDate: startDate.toISOString().split('T')[0],
      endDate: endDate.toISOString().split('T')[0]
    });
  };

  // Aggregate data for visualizations
  const getDailyStats = () => {
    const filteredQueries = getFilteredQueries();
    const dailyMap = {};
    filteredQueries.forEach(q => {
      if (!dailyMap[q.date]) {
        dailyMap[q.date] = { date: q.date, queries: 0, data_scanned: 0 };
      }
      dailyMap[q.date].queries += q.query_count;
      dailyMap[q.date].data_scanned += q.scanned_size_mb;
    });
    // Sort by date chronologically (oldest to newest) and convert MB to TB
    return Object.values(dailyMap)
      .sort((a, b) => new Date(a.date) - new Date(b.date))
      .map(day => ({
        ...day,
        data_scanned: day.data_scanned / (1024 * 1024) // Convert MB to TB
      }));
  };

  const getWorkgroupStats = () => {
    const filteredQueries = getFilteredQueries();
    const workgroupMap = {};
    filteredQueries.forEach(q => {
      if (!workgroupMap[q.workgroup]) {
        workgroupMap[q.workgroup] = { name: q.workgroup, queries: 0, data: 0 };
      }
      workgroupMap[q.workgroup].queries += q.query_count;
      workgroupMap[q.workgroup].data += q.scanned_size_mb;
    });
    // Sort by queries descending, take top 10, and convert MB to TB
    return Object.values(workgroupMap)
      .sort((a, b) => b.queries - a.queries)
      .slice(0, 10)
      .map(wg => ({
        ...wg,
        data: wg.data / (1024 * 1024) // Convert MB to TB
      }));
  };

  const getWorkgroupDataUsage = () => {
    const filteredQueries = getFilteredQueries();
    const workgroupMap = {};
    filteredQueries.forEach(q => {
      if (!workgroupMap[q.workgroup]) {
        workgroupMap[q.workgroup] = { name: q.workgroup, queries: 0, data: 0 };
      }
      workgroupMap[q.workgroup].queries += q.query_count;
      workgroupMap[q.workgroup].data += q.scanned_size_mb;
    });
    // Sort by data descending, take top 10, and convert MB to TB
    return Object.values(workgroupMap)
      .sort((a, b) => b.data - a.data)
      .slice(0, 10)
      .map(wg => ({
        ...wg,
        data: wg.data / (1024 * 1024) // Convert MB to TB
      }));
  };

  // Get daily query stats by workgroup (top 10 by total queries)
  const getDailyStatsByWorkgroupQueries = () => {
    const filteredQueries = getFilteredQueries();
    
    // First, get top 10 workgroups by total queries
    const workgroupTotals = {};
    filteredQueries.forEach(q => {
      if (!workgroupTotals[q.workgroup]) {
        workgroupTotals[q.workgroup] = 0;
      }
      workgroupTotals[q.workgroup] += q.query_count;
    });
    
    const topWorkgroups = Object.entries(workgroupTotals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([name]) => name);
    
    // Get dates in range
    const dates = [];
    if (dateRange.startDate && dateRange.endDate) {
      const start = new Date(dateRange.startDate);
      const end = new Date(dateRange.endDate);
      const current = new Date(start);
      while (current <= end) {
        dates.push(current.toISOString().split('T')[0]);
        current.setDate(current.getDate() + 1);
      }
    }
    
    // Build data structure: array of { date, workgroup1: count, workgroup2: count, ... }
    const dailyMap = {};
    dates.forEach(date => {
      dailyMap[date] = { date };
      topWorkgroups.forEach(wg => {
        dailyMap[date][wg] = 0;
      });
    });
    
    // Populate with actual data
    filteredQueries.forEach(q => {
      if (topWorkgroups.includes(q.workgroup) && dailyMap[q.date]) {
        dailyMap[q.date][q.workgroup] = (dailyMap[q.date][q.workgroup] || 0) + q.query_count;
      }
    });
    
    return Object.values(dailyMap).sort((a, b) => new Date(a.date) - new Date(b.date));
  };

  // Get daily data scanned stats by workgroup (top 10 by total data scanned)
  const getDailyStatsByWorkgroupData = () => {
    const filteredQueries = getFilteredQueries();
    
    // First, get top 10 workgroups by total data scanned
    const workgroupTotals = {};
    filteredQueries.forEach(q => {
      if (!workgroupTotals[q.workgroup]) {
        workgroupTotals[q.workgroup] = 0;
      }
      workgroupTotals[q.workgroup] += q.scanned_size_mb;
    });
    
    const topWorkgroups = Object.entries(workgroupTotals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([name]) => name);
    
    // Get dates in range
    const dates = [];
    if (dateRange.startDate && dateRange.endDate) {
      const start = new Date(dateRange.startDate);
      const end = new Date(dateRange.endDate);
      const current = new Date(start);
      while (current <= end) {
        dates.push(current.toISOString().split('T')[0]);
        current.setDate(current.getDate() + 1);
      }
    }
    
    // Build data structure: array of { date, workgroup1: tb, workgroup2: tb, ... }
    const dailyMap = {};
    dates.forEach(date => {
      dailyMap[date] = { date };
      topWorkgroups.forEach(wg => {
        dailyMap[date][wg] = 0;
      });
    });
    
    // Populate with actual data (convert MB to TB)
    filteredQueries.forEach(q => {
      if (topWorkgroups.includes(q.workgroup) && dailyMap[q.date]) {
        dailyMap[q.date][q.workgroup] = (dailyMap[q.date][q.workgroup] || 0) + q.scanned_size_mb;
      }
    });
    
    // Convert MB to TB
    return Object.values(dailyMap)
      .sort((a, b) => new Date(a.date) - new Date(b.date))
      .map(day => {
        const result = { date: day.date };
        topWorkgroups.forEach(wg => {
          result[wg] = day[wg] / (1024 * 1024); // Convert MB to TB
        });
        return result;
      });
  };

  // Get all unique workgroups for filter dropdown
  const getWorkgroups = () => {
    const workgroups = new Set(queries.map(q => q.workgroup).filter(Boolean));
    return Array.from(workgroups).sort();
  };

  const getTotalStats = () => {
    const filteredQueries = getFilteredQueries();
    const total = filteredQueries.reduce((acc, q) => ({
      queries: acc.queries + q.query_count,
      data: acc.data + q.scanned_size_mb,
      time: acc.time + (q.avg_execution_time * q.query_count)
    }), { queries: 0, data: 0, time: 0 });

    return {
      total_queries: total.queries,
      total_data_tb: (total.data / (1024 * 1024)).toFixed(2), // Convert MB to TB
      avg_time: total.queries > 0 ? ((total.time / total.queries) / 60).toFixed(1) : '0.0', // Convert seconds to minutes
      workgroups: new Set(filteredQueries.map(q => q.workgroup)).size
    };
  };

  const handleSendMessage = async () => {
    if (!inputMessage.trim()) return;

    const userMessage = { role: 'user', content: inputMessage };
    const currentMessage = inputMessage;
    setChatMessages(prev => [...prev, userMessage]);
    setInputMessage('');
    setChatLoading(true);

    try {
      // Call backend API which will handle MCP integration
      const response = await fetch('http://localhost:8000/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: currentMessage,
          chat_history: chatMessages
        })
      });

      if (response.ok) {
        const data = await response.json();
        const assistantMessage = {
          role: 'assistant',
          content: data.response
        };
        setChatMessages(prev => [...prev, assistantMessage]);
      } else {
        throw new Error('Failed to get response');
      }
    } catch (error) {
      console.error('Chat error:', error);
      setChatMessages(prev => [...prev, {
        role: 'assistant',
        content: 'Sorry, I encountered an error. Please try again.'
      }]);
    } finally {
      setChatLoading(false);
    }
  };

  const stats = getTotalStats();
  const dailyStats = getDailyStats();
  const workgroupStats = getWorkgroupStats();
  const workgroupDataUsage = getWorkgroupDataUsage();
  const dailyStatsByWorkgroupQueries = getDailyStatsByWorkgroupQueries();
  const dailyStatsByWorkgroupData = getDailyStatsByWorkgroupData();
  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#a855f7'];

  // Get list of workgroups from the data
  const getWorkgroupNames = () => {
    if (dailyStatsByWorkgroupQueries.length > 0) {
      return Object.keys(dailyStatsByWorkgroupQueries[0]).filter(key => key !== 'date');
    }
    return [];
  };

  const workgroupNames = getWorkgroupNames();

  // Handle legend click to filter workgroups
  const handleLegendClick = (e) => {
    const clickedWorkgroup = e.dataKey;
    // If clicking the same workgroup, reset to show all; otherwise show only clicked one
    if (selectedWorkgroupLine === clickedWorkgroup) {
      setSelectedWorkgroupLine(null);
    } else {
      setSelectedWorkgroupLine(clickedWorkgroup);
    }
  };

  // Filter workgroups based on selection
  const getVisibleWorkgroups = () => {
    if (selectedWorkgroupLine === null) {
      return workgroupNames; // Show all
    }
    return [selectedWorkgroupLine]; // Show only selected
  };

  const visibleWorkgroups = getVisibleWorkgroups();

  // Format date range for display
  const getDateRangeDisplay = () => {
    if (!dateRange.startDate || !dateRange.endDate) return '';
    const start = new Date(dateRange.startDate);
    const end = new Date(dateRange.endDate);
    const startStr = `${start.getMonth() + 1}/${start.getDate()}/${start.getFullYear()}`;
    const endStr = `${end.getMonth() + 1}/${end.getDate()}/${end.getFullYear()}`;
    return `${startStr} - ${endStr}`;
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 text-white flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-400 mx-auto mb-4"></div>
          <p className="text-slate-400">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 text-white p-6">
      {/* Header */}
      <div className="max-w-7xl mx-auto mb-8">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-4xl font-bold mb-2 bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
              AWS Athena Analytics
            </h1>
            <p className="text-slate-400">Query performance and cost monitoring dashboard</p>
          </div>
          <button
            onClick={() => setChatOpen(!chatOpen)}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 px-6 py-3 rounded-lg transition-colors"
          >
            <MessageSquare size={20} />
            <span>Ask AI</span>
          </button>
        </div>
        {/* Filters */}
        <div className="flex flex-wrap items-center gap-4">
          {/* Workgroup Filter */}
          <div className="flex items-center gap-3">
            <Filter className="text-slate-400" size={20} />
            <label htmlFor="workgroup-filter" className="text-slate-400 text-sm font-medium">
              Workgroup:
            </label>
            <select
              id="workgroup-filter"
              value={selectedWorkgroup}
              onChange={(e) => setSelectedWorkgroup(e.target.value)}
              className="bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500 min-w-[200px]"
            >
              <option value="all">All Workgroups</option>
              {getWorkgroups().map(wg => (
                <option key={wg} value={wg}>{wg}</option>
              ))}
            </select>
          </div>

          {/* Date Range Filter */}
          <div className="flex items-center gap-3">
            <Calendar className="text-slate-400" size={20} />
            <label htmlFor="start-date" className="text-slate-400 text-sm font-medium">
              Date Range:
            </label>
            <input
              type="date"
              id="start-date"
              value={dateRange.startDate}
              onChange={(e) => setDateRange({ ...dateRange, startDate: e.target.value })}
              className="bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:border-blue-500"
            />
            <span className="text-slate-400">to</span>
            <input
              type="date"
              id="end-date"
              value={dateRange.endDate}
              onChange={(e) => setDateRange({ ...dateRange, endDate: e.target.value })}
              className="bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:border-blue-500"
            />
          </div>

          {/* Quick Date Selectors */}
          <div className="flex items-center gap-2">
            <span className="text-slate-400 text-sm font-medium">Quick:</span>
            <button
              onClick={() => setQuickDateRange('1D')}
              className="bg-slate-700 hover:bg-slate-600 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm transition-colors"
            >
              1D
            </button>
            <button
              onClick={() => setQuickDateRange('1W')}
              className="bg-slate-700 hover:bg-slate-600 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm transition-colors"
            >
              1W
            </button>
            <button
              onClick={() => setQuickDateRange('1M')}
              className="bg-slate-700 hover:bg-slate-600 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm transition-colors"
            >
              1M
            </button>
          </div>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="max-w-7xl mx-auto grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-2">
            <Database className="text-blue-400" size={18} />
            <h3 className="text-slate-400 text-xs font-medium">Total Queries</h3>
          </div>
          <p className="text-2xl font-bold">{stats.total_queries.toLocaleString()}</p>
        </div>

        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="text-green-400" size={18} />
            <h3 className="text-slate-400 text-xs font-medium">Data Scanned</h3>
          </div>
          <p className="text-2xl font-bold">{stats.total_data_tb} TB</p>
        </div>

        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-2">
            <Calendar className="text-yellow-400" size={18} />
            <h3 className="text-slate-400 text-xs font-medium">Avg Exec Time</h3>
          </div>
          <p className="text-2xl font-bold">{stats.avg_time} mins</p>
        </div>

        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-2">
            <Users className="text-purple-400" size={18} />
            <h3 className="text-slate-400 text-xs font-medium">Workgroups</h3>
          </div>
          <p className="text-2xl font-bold">{stats.workgroups}</p>
        </div>
      </div>

      {/* Charts */}
      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Query Trend */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-6">
          <h3 className="text-xl font-semibold mb-4">Query Trend ({getDateRangeDisplay()})</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={dailyStats}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis 
                dataKey="date" 
                stroke="#94a3b8"
                tick={{ fill: '#94a3b8' }}
                tickFormatter={(val) => {
                  const date = new Date(val);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#94a3b8" tick={{ fill: '#94a3b8' }} />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
                labelStyle={{ color: '#cbd5e1' }}
              />
              <Legend wrapperStyle={{ color: '#cbd5e1' }} />
              <Line type="monotone" dataKey="queries" stroke="#3b82f6" strokeWidth={2} name="Queries" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Data Scanned Trend */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-6">
          <h3 className="text-xl font-semibold mb-4">Data Scanned (TB)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={dailyStats}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis 
                dataKey="date" 
                stroke="#94a3b8"
                tick={{ fill: '#94a3b8' }}
                tickFormatter={(val) => {
                  const date = new Date(val);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#94a3b8" tick={{ fill: '#94a3b8' }} />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
                labelStyle={{ color: '#cbd5e1' }}
                formatter={(value) => `${Number(value).toFixed(2)} TB`}
              />
              <Legend wrapperStyle={{ color: '#cbd5e1' }} />
              <Bar dataKey="data_scanned" fill="#10b981" name="Data Scanned (TB)" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Workgroup Trend Charts */}
      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Query Trend by Workgroup */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-xl font-semibold">Query Trend by Workgroup (Top 10)</h3>
            {selectedWorkgroupLine && (
              <button
                onClick={() => setSelectedWorkgroupLine(null)}
                className="text-xs text-blue-400 hover:text-blue-300 underline"
              >
                Show All
              </button>
            )}
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={dailyStatsByWorkgroupQueries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis 
                dataKey="date" 
                stroke="#94a3b8"
                tick={{ fill: '#94a3b8' }}
                tickFormatter={(val) => {
                  const date = new Date(val);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#94a3b8" tick={{ fill: '#94a3b8' }} />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
                labelStyle={{ color: '#cbd5e1' }}
              />
              <Legend 
                wrapperStyle={{ color: '#cbd5e1' }} 
                onClick={handleLegendClick}
                style={{ cursor: 'pointer' }}
              />
              {dailyStatsByWorkgroupQueries.length > 0 && workgroupNames
                .filter(workgroup => visibleWorkgroups.includes(workgroup))
                .map((workgroup, index) => {
                  const originalIndex = workgroupNames.indexOf(workgroup);
                  return (
                    <Line 
                      key={workgroup}
                      type="monotone" 
                      dataKey={workgroup} 
                      stroke={COLORS[originalIndex % COLORS.length]} 
                      strokeWidth={selectedWorkgroupLine === workgroup ? 3 : 2}
                      name={workgroup}
                      dot={false}
                      style={{ cursor: 'pointer' }}
                      onClick={() => handleLegendClick({ dataKey: workgroup })}
                    />
                  );
                })}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Data Scanned Trend by Workgroup */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-xl font-semibold">Data Scanned by Workgroup (Top 10)</h3>
            {selectedWorkgroupLine && (
              <button
                onClick={() => setSelectedWorkgroupLine(null)}
                className="text-xs text-blue-400 hover:text-blue-300 underline"
              >
                Show All
              </button>
            )}
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={dailyStatsByWorkgroupData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis 
                dataKey="date" 
                stroke="#94a3b8"
                tick={{ fill: '#94a3b8' }}
                tickFormatter={(val) => {
                  const date = new Date(val);
                  return `${date.getMonth() + 1}/${date.getDate()}`;
                }}
              />
              <YAxis stroke="#94a3b8" tick={{ fill: '#94a3b8' }} />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
                labelStyle={{ color: '#cbd5e1' }}
                formatter={(value) => `${Number(value).toFixed(2)} TB`}
              />
              <Legend 
                wrapperStyle={{ color: '#cbd5e1' }} 
                onClick={handleLegendClick}
                style={{ cursor: 'pointer' }}
              />
              {dailyStatsByWorkgroupData.length > 0 && workgroupNames
                .filter(workgroup => visibleWorkgroups.includes(workgroup))
                .map((workgroup, index) => {
                  const originalIndex = workgroupNames.indexOf(workgroup);
                  return (
                    <Line 
                      key={workgroup}
                      type="monotone" 
                      dataKey={workgroup} 
                      stroke={COLORS[originalIndex % COLORS.length]} 
                      strokeWidth={selectedWorkgroupLine === workgroup ? 3 : 2}
                      name={workgroup}
                      dot={false}
                      style={{ cursor: 'pointer' }}
                      onClick={() => handleLegendClick({ dataKey: workgroup })}
                    />
                  );
                })}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts */}
      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Workgroup Distribution */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-6">
          <h3 className="text-xl font-semibold mb-4">Queries by Workgroup (Top 10)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={workgroupStats}
                dataKey="queries"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={100}
                label={(entry) => `${entry.name}: ${entry.queries}`}
              >
                {workgroupStats.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Workgroup Data Usage */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl p-6">
          <h3 className="text-xl font-semibold mb-4">Data Usage by Workgroup (TB)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={workgroupDataUsage} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" stroke="#94a3b8" tick={{ fill: '#94a3b8' }} />
              <YAxis 
                type="category" 
                dataKey="name" 
                stroke="#94a3b8" 
                tick={{ fill: '#94a3b8', fontSize: 11 }}
                width={150}
              />
              <Tooltip 
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155' }}
                labelStyle={{ color: '#cbd5e1' }}
                formatter={(value) => `${Number(value).toFixed(2)} TB`}
              />
              <Bar dataKey="data" fill="#f59e0b" name="Data (TB)" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Chat Interface */}
      {chatOpen && (
        <div className="fixed bottom-6 right-6 w-96 h-[600px] bg-slate-800 border border-slate-700 rounded-xl shadow-2xl flex flex-col z-50">
          <div className="flex items-center justify-between p-4 border-b border-slate-700">
            <h3 className="font-semibold">AI Query Assistant</h3>
            <button onClick={() => setChatOpen(false)} className="text-slate-400 hover:text-white">
              <X size={20} />
            </button>
          </div>
          
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {chatMessages.length === 0 && (
              <div className="text-center text-slate-400 mt-8">
                <MessageSquare size={48} className="mx-auto mb-4 opacity-50" />
                <p>Ask me about your Athena queries!</p>
                <p className="text-sm mt-2">Try: "Show me the most expensive queries" or "Which workgroup uses the most data?"</p>
              </div>
            )}
            {chatMessages.map((msg, idx) => (
              <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                <div className={`max-w-[80%] rounded-lg p-3 ${
                  msg.role === 'user' 
                    ? 'bg-blue-600 text-white' 
                    : 'bg-slate-700 text-slate-100'
                }`}>
                  {msg.content}
                </div>
              </div>
            ))}
            {chatLoading && (
              <div className="flex justify-start">
                <div className="bg-slate-700 rounded-lg p-3">
                  <div className="flex gap-2">
                    <div className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                    <div className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                    <div className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                  </div>
                </div>
              </div>
            )}
          </div>
          
          <div className="p-4 border-t border-slate-700">
            <div className="flex gap-2">
              <input
                type="text"
                value={inputMessage}
                onChange={(e) => setInputMessage(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && !chatLoading && handleSendMessage()}
                placeholder="Ask about your queries..."
                className="flex-1 bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 focus:outline-none focus:border-blue-500 text-white"
              />
              <button
                onClick={handleSendMessage}
                disabled={chatLoading || !inputMessage.trim()}
                className="bg-blue-600 hover:bg-blue-700 disabled:bg-slate-600 disabled:cursor-not-allowed p-2 rounded-lg transition-colors"
              >
                <Send size={20} />
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AthenaQueryDashboard;

