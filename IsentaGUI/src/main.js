const invoke = window.__TAURI_INTERNALS__.invoke;

document.addEventListener('DOMContentLoaded', () => {
    const queryInput = document.getElementById('query-input');
    const submitButton = document.getElementById('submit-query');
    const queryResultContainer = document.getElementById('query-result');
    const schemaContainer = document.getElementById('schema-container');
    const refreshSchemaButton = document.getElementById('refresh-schema');

    // --- Query Execution ---

    const executeQuery = async () => {
        const query = queryInput.value.trim();
        if (!query) {
            renderMessage('Please enter a query.');
            return;
        }

        renderMessage('Executing query...');

        try {
            const result = await invoke('execute_query', { query });
            renderResult(result || '[No output from server]');
        } catch (error) {
            renderError(error);
        }
    };

    submitButton.addEventListener('click', executeQuery);

    queryInput.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            executeQuery();
        }
    });

    // --- Result Rendering ---

    function renderMessage(message) {
        queryResultContainer.innerHTML = `<p class="placeholder-text">${message}</p>`;
    }

    function renderError(error) {
        queryResultContainer.innerHTML = `<pre class="error-text">${error}</pre>`;
    }

    function renderResult(resultText) {
        queryResultContainer.innerHTML = '';
        const lines = resultText.trim().split('\n');

        // Simple check for tabular data: header + separator + at least one row
        if (lines.length < 2 || !lines[0].includes('|')) {
            renderMessage(resultText);
            return;
        }

        const table = document.createElement('table');
        const thead = document.createElement('thead');
        const tbody = document.createElement('tbody');

        // Header
        const headerRow = document.createElement('tr');
        const headers = lines[0].split('|').map(h => h.trim());
        headers.forEach(headerText => {
            const th = document.createElement('th');
            th.textContent = headerText;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);

        // Body (skipping the separator line)
        for (let i = 2; i < lines.length; i++) {
            const dataRow = document.createElement('tr');
            const values = lines[i].split('|').map(v => v.trim());
            values.forEach(valueText => {
                const td = document.createElement('td');
                td.textContent = valueText;
                dataRow.appendChild(td);
            });
            tbody.appendChild(dataRow);
        }

        table.appendChild(thead);
        table.appendChild(tbody);
        queryResultContainer.appendChild(table);
    }

    // --- Schema Handling ---

    const refreshSchema = async () => {
        schemaContainer.innerHTML = '<p>Loading...</p>';
        try {
            const result = await invoke('execute_query', { query: 'SHOW TABLES' });
            
            const lines = result.trim().split('\n');
            // Expecting result like:
            // Tables:
            // - table1
            // - table2
            if (lines.length < 2 || !lines[0].toLowerCase().startsWith('tables')) {
                 if (result.toLowerCase().includes("no tables")) {
                    schemaContainer.innerHTML = '<p>No tables found.</p>';
                } else {
                    schemaContainer.innerHTML = `<p class="error-text">Unexpected format</p>`;
                }
                return;
            }

            const tableList = document.createElement('ul');
            for (let i = 1; i < lines.length; i++) {
                const tableName = lines[i].replace('-', '').trim();
                if (tableName) {
                    const li = document.createElement('li');
                    li.textContent = tableName;
                    li.dataset.tableName = tableName;
                    tableList.appendChild(li);
                }
            }
            schemaContainer.innerHTML = '';
            schemaContainer.appendChild(tableList);

        } catch (error) {
            schemaContainer.innerHTML = `<p class="error-text">Failed to load schema.</p>`;
        }
    };
    
    refreshSchemaButton.addEventListener('click', refreshSchema);

    // Add query suggestion on table name click
    schemaContainer.addEventListener('click', (e) => {
        if (e.target && e.target.nodeName === 'LI') {
            const tableName = e.target.dataset.tableName;
            queryInput.value = `SELECT * FROM ${tableName}`;
            queryInput.focus();
        }
    });

    // Initial load
    refreshSchema();
});
