// Extracted from dashboard.html <script> blocks
// All dashboard logic, event handlers, and functions go here.
// WebSocket connection and global state
const socket = io();
let allChannelsData = [];
let currentChannelData = null;
let selectedChannel = null;
let currentDateFilter = null;
let availableDates = [];
let sortDirection = {};

// Enhanced date filtering state
let filterMode = "range"; // 'range', 'include', 'exclude'
let selectedDates = new Set();
let excludedDates = new Set();

// DOM elements
const statusDot = document.getElementById("connectionStatus");
const statusText = document.getElementById("connectionText");
const lastUpdateElement = document.getElementById("lastUpdate");
const channelSelect = document.getElementById("channelSelect");
const dateFilterInput = document.getElementById("dateFilterInput");
const dateSuggestions = document.getElementById("dateSuggestions");

// Enhanced Date Filter Functions
function onFilterModeChange() {
	const modeSelect = document.getElementById("filterModeSelect");
	const dateFilterLabel = document.getElementById("dateFilterLabel");
	const selectedDatesContainer = document.getElementById(
		"selectedDatesContainer"
	);
	const selectedDatesLabel = document.getElementById("selectedDatesLabel");

	filterMode = modeSelect.value;

	// Clear previous selections
	selectedDates.clear();
	excludedDates.clear();
	updateSelectedDatesDisplay();

	// Update UI based on mode
	switch (filterMode) {
		case "range":
			dateFilterLabel.textContent = "Date Range";
			dateFilterInput.placeholder = "2024-01-01 or 2024-01-01:2024-01-31";
			selectedDatesContainer.style.display = "none";
			dateFilterInput.style.display = "block";
			break;
		case "include":
			dateFilterLabel.textContent = "Select Dates to Include";
			dateFilterInput.placeholder = "Search for dates to include...";
			selectedDatesContainer.style.display = "block";
			selectedDatesLabel.textContent = "Selected Dates:";
			dateFilterInput.style.display = "block";
			break;
		case "exclude":
			dateFilterLabel.textContent = "Select Dates to Exclude";
			dateFilterInput.placeholder = "Search for dates to exclude...";
			selectedDatesContainer.style.display = "block";
			selectedDatesLabel.textContent = "Excluded Dates:";
			dateFilterInput.style.display = "block";
			break;
	}

	// Add help text
	addFilterModeHelp();
	updateDateSuggestions();
}

function addFilterModeHelp() {
	// Remove existing help text
	const existingHelp = document.querySelector(".filter-mode-help");
	if (existingHelp) {
		existingHelp.remove();
	}

	const dateFilterGroup = document.getElementById("dateFilterGroup");
	const helpDiv = document.createElement("div");
	helpDiv.className = "filter-mode-help";

	switch (filterMode) {
		case "range":
			helpDiv.innerHTML =
				'<i class="fas fa-info-circle"></i> Enter a single date (2024-01-01) or range (2024-01-01:2024-01-31)';
			break;
		case "include":
			helpDiv.innerHTML =
				'<i class="fas fa-info-circle"></i> Click dates below to include only those dates in the analysis';
			break;
		case "exclude":
			helpDiv.innerHTML =
				'<i class="fas fa-info-circle"></i> Click dates below to exclude them from the analysis (all others included)';
			break;
	}

	dateFilterGroup.appendChild(helpDiv);
}

function clearSelectedDates() {
	selectedDates.clear();
	excludedDates.clear();
	updateSelectedDatesDisplay();
	updateDateSuggestions();
}

function updateSelectedDatesDisplay() {
	const selectedDatesList = document.getElementById("selectedDatesList");
	const currentSet = filterMode === "exclude" ? excludedDates : selectedDates;

	selectedDatesList.innerHTML = "";

	if (currentSet.size === 0) {
		selectedDatesList.innerHTML =
			'<span style="color: var(--text-muted); font-style: italic;">None selected</span>';
		return;
	}

	const sortedDates = Array.from(currentSet).sort();
	sortedDates.forEach((date) => {
		const chip = document.createElement("div");
		chip.className = `selected-date-chip ${
			filterMode === "exclude" ? "exclude-mode" : ""
		}`;
		chip.innerHTML = `
						<span>${date}</span>
						<button class="remove-btn" onclick="removeSelectedDate('${date}')" type="button">
							<i class="fas fa-times"></i>
						</button>
					`;
		selectedDatesList.appendChild(chip);
	});
}

function removeSelectedDate(date) {
	if (filterMode === "exclude") {
		excludedDates.delete(date);
	} else {
		selectedDates.delete(date);
	}
	updateSelectedDatesDisplay();
	updateDateSuggestions();
}

function toggleDateSelection(date, event) {
	if (event) {
		event.preventDefault();
		event.stopPropagation();
	}

	if (filterMode === "range") {
		// For range mode, set the input value
		dateFilterInput.value = date;
		hideDateSuggestions();
		return;
	}

	const currentSet = filterMode === "exclude" ? excludedDates : selectedDates;

	if (currentSet.has(date)) {
		currentSet.delete(date);
	} else {
		currentSet.add(date);
	}

	updateSelectedDatesDisplay();
	updateDateSuggestions();

	// Keep suggestions open for multi-select modes
	setTimeout(showDateSuggestions, 10);
}

function buildDateFilterString() {
	switch (filterMode) {
		case "range":
			return dateFilterInput.value.trim() || null;
		case "include":
			if (selectedDates.size === 0) return null;
			return `include:${Array.from(selectedDates).sort().join(",")}`;
		case "exclude":
			if (excludedDates.size === 0) return null;
			return `exclude:${Array.from(excludedDates).sort().join(",")}`;
		default:
			return null;
	}
}

// Socket event handlers
socket.on("connect", function () {
	statusDot.classList.remove("disconnected");
	statusText.textContent = "Connected";
	console.log("Connected to server");
});

socket.on("disconnect", function () {
	statusDot.classList.add("disconnected");
	statusText.textContent = "Disconnected";
	console.log("Disconnected from server");
});

socket.on("data_update", function (data) {
	console.log("Data update received:", data);
	allChannelsData = data.channels;
	updateChannelsOverview();
	updateLastUpdateTime();

	if (selectedChannel) {
		requestChannelData(selectedChannel, currentDateFilter);
	}
});

socket.on("channel_data", function (data) {
	console.log("Channel data received:", data);
	// Safeguard: ensure data.channel is a string
	if (typeof data.channel !== "string") {
		if (data.channel && typeof data.channel === "object") {
			// Try to extract channel name if possible
			if (data.channel.name) {
				data.channel = data.channel.name;
			} else {
				data.channel = JSON.stringify(data.channel);
			}
		} else {
			data.channel = String(data.channel);
		}
		console.warn("Fixed channel field to string:", data.channel);
	}
	currentChannelData = data;
	displayChannelData(data);
	// Update side menu overview so the selected channel card reflects filtered data
	updateChannelsOverview();
});

socket.on("error", function (data) {
	console.error("Socket error:", data);
	showNotification("Error: " + data.message, "error");
});

// Load channels on page load
document.addEventListener("DOMContentLoaded", function () {
	loadChannels();
});

async function loadChannels() {
	try {
		const response = await fetch("/api/summary");
		const data = await response.json();
		allChannelsData = data.channels;
		updateChannelsOverview();
		populateChannelSelect();
		// Select the first channel by default if available
		if (allChannelsData.length > 0) {
			const firstChannel = allChannelsData[0].channel;
			channelSelect.value = firstChannel;
			onChannelChange();
		}
	} catch (error) {
		console.error("Error loading channels:", error);
		showNotification("Failed to load channels", "error");
	}
}

function populateChannelSelect() {
	channelSelect.innerHTML = '<option value="">📺 Choose a channel...</option>';
	allChannelsData.forEach((channel) => {
		const option = document.createElement("option");
		option.value = channel.channel;
		// Compact display: icon + name only
		option.textContent = `📺 ${channel.channel}`;
		channelSelect.appendChild(option);
	});
}

function updateChannelsOverview() {
	const container = document.getElementById("channelsOverview");
	container.innerHTML = "";

	if (selectedChannel) {
		const channelObj = allChannelsData.find(
			(ch) => ch.channel === selectedChannel
		);
		if (channelObj) {
			const card = createChannelCard(channelObj);
			container.appendChild(card);
		}
	} else {
		allChannelsData.forEach((channel) => {
			const card = createChannelCard(channel);
			container.appendChild(card);
		});
	}
}

function createChannelCard(channel) {
	const card = document.createElement("div");
	card.className = `channel-card ${
		selectedChannel === channel.channel ? "selected" : ""
	}`;

	// Compact card with just icon and name
	card.innerHTML = `
					<div class="channel-compact">
						<div class="channel-icon">📺</div>
						<div class="channel-info">
							<div class="channel-name">#${channel.channel}</div>
							<div class="channel-status ${channel.needs_update ? "outdated" : "updated"}">
								${channel.needs_update ? "⚠️ Update needed" : "✅ Up to date"}
							</div>
						</div>
					</div>
				`;

	// Show detailed stats only for selected channel
	if (selectedChannel === channel.channel) {
		// If a date filter is active and we have filtered channel data, prefer that for the side-menu
		const detailsSource =
			currentChannelData &&
			currentChannelData.channel === channel.channel &&
			currentDateFilter
				? currentChannelData
				: channel;

		const detailsDiv = document.createElement("div");
		detailsDiv.className = "channel-details";
		detailsDiv.innerHTML = `
						<div class="channel-stats">
							<div class="stat-item">
								<span class="stat-value">${(
									detailsSource.total_users || 0
								).toLocaleString()}</span>
								<span class="stat-label">Total Users</span>
							</div>
							<div class="stat-item">
								<span class="stat-value">${(
									detailsSource.unique_user_count || 0
								).toLocaleString()}</span>
								<span class="stat-label">Unique Users</span>
							</div>
							<div class="stat-item">
								<span class="stat-value">${(
									detailsSource.total_messages || 0
								).toLocaleString()}</span>
								<span class="stat-label">Messages</span>
							</div>
							<div class="stat-item">
								<span class="stat-value">${detailsSource.start_date || "-"}</span>
								<span class="stat-label">to ${detailsSource.end_date || "-"}</span>
							</div>
						</div>
					`;
		card.appendChild(detailsDiv);
	}

	// card is no longer clickable
	return card;
}

async function selectChannel(channelName) {
	selectedChannel = channelName;
	channelSelect.value = channelName;
	updateChannelsOverview();

	// Load available dates for this channel
	await loadAvailableDates(channelName);

	// Request channel data
	requestChannelData(channelName, currentDateFilter);
}

async function loadAvailableDates(channelName) {
	try {
		const response = await fetch(`/api/channel/${channelName}/dates`);
		const data = await response.json();
		availableDates = data.dates;
		updateDateSuggestions();
	} catch (error) {
		console.error("Error loading dates:", error);
	}
}

function onChannelChange() {
	const channelName = channelSelect.value;
	const channelInfo = document.getElementById("selectedChannelInfo");

	if (channelName) {
		// Show channel info
		updateSelectedChannelInfo(channelName);
		channelInfo.style.display = "block";
		selectChannel(channelName);
	} else {
		// Hide channel info
		channelInfo.style.display = "none";
		selectedChannel = null;
		currentDateFilter = null;
		dateFilterInput.value = "";
		document.getElementById("dataTableSection").style.display = "none";
		updateChannelsOverview();
	}
}

function updateSelectedChannelInfo(channelName) {
	// Find channel data
	const channelData = allChannelsData.find((ch) => ch.channel === channelName);
	if (!channelData) return;

	// Update the title
	document.getElementById(
		"selectedChannelTitle"
	).textContent = `#${channelName} Details`;

	// Update status badge
	const statusBadge = document.getElementById("channelStatusBadge");
	if (channelData.needs_update) {
		statusBadge.textContent = "⚠️ Update needed";
		statusBadge.className = "status-badge outdated";
	} else {
		statusBadge.textContent = "✅ Up to date";
		statusBadge.className = "status-badge";
	}

	// Update the info display
	document.getElementById("channelUserCount").textContent =
		channelData.total_users?.toLocaleString() || "-";
	document.getElementById("channelMessageCount").textContent =
		channelData.total_messages?.toLocaleString() || "-";

	if (channelData.start_date && channelData.end_date) {
		const startDate = new Date(channelData.start_date).toLocaleDateString();
		const endDate = new Date(channelData.end_date).toLocaleDateString();
		document.getElementById(
			"channelDateRange"
		).textContent = `${startDate} - ${endDate}`;
	} else {
		document.getElementById("channelDateRange").textContent = "-";
	}
}

function requestChannelData(channelName, dateFilter = null) {
	document.getElementById("loadingSpinner").style.display = "block";
	document.getElementById("dataTable").style.display = "none";
	document.getElementById("dataTableSection").style.display = "block";

	socket.emit("request_channel_data", {
		channel: channelName,
		date_filter: dateFilter,
	});
}

function displayChannelData(data) {
	document.getElementById("loadingSpinner").style.display = "none";
	document.getElementById("dataTable").style.display = "table";

	// Update section title
	document.getElementById(
		"sectionTitle"
	).textContent = `${data.channel} Analytics`;

	// Update summary cards. If a date filter is active, prefer the cached unfiltered totals
	// from allChannelsData so the header shows overall channel totals while the table
	// displays the filtered user-level data.
	const summaryContainer = document.getElementById("channelSummary");
	let summarySource = data;
	if (currentDateFilter) {
		const cached = allChannelsData.find((ch) => ch.channel === data.channel);
		if (cached) summarySource = cached;
	}

	summaryContainer.innerHTML = `
				<div class="summary-card">
					<div class="stat-value">${(
						summarySource.total_users || 0
					).toLocaleString()}</div>
					<div class="stat-label">Total Users</div>
				</div>
				<div class="summary-card">
					<div class="stat-value">${(
						summarySource.unique_user_count || 0
					).toLocaleString()}</div>
					<div class="stat-label">Unique Users</div>
				</div>
				<div class="summary-card">
					<div class="stat-value">${(
						summarySource.total_messages || 0
					).toLocaleString()}</div>
					<div class="stat-label">Messages</div>
				</div>
				<div class="summary-card">
					<div class="stat-value">${summarySource.start_date || "-"}</div>
					<div class="stat-label">to ${summarySource.end_date || "-"}</div>
				</div>
			`;

	// Populate table
	const tbody = document.getElementById("tableBody");
	tbody.innerHTML = "";

	data.user_stats.forEach((user) => {
		const row = document.createElement("tr");

		// Determine alt likelihood class
		let altClass = "low";
		if (user.alt_likelihood > 70) altClass = "high";
		else if (user.alt_likelihood > 40) altClass = "medium";

		// Format last updated time
		const lastActive = new Date(user.last_updated).toLocaleString();

		row.innerHTML = `
                    <td><strong><a href="#" onclick="openUserDetail('${user.username.replace(
											/'/g,
											"\\'"
										)}'); return false;" style="color: var(--primary-color); text-decoration: none; cursor: pointer; transition: var(--transition);" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">${
			user.username
		}</a></strong></td>
                    <td>${user.chat_count.toLocaleString()}</td>
                    <td><span class="alt-badge ${altClass}">${user.alt_likelihood.toFixed(
			1
		)}%</span></td>
                    <td><span style="color: var(--text-secondary); font-size: 0.875rem;">${lastActive}</span></td>
                `;

		tbody.appendChild(row);
	});
}

function showDateSuggestions() {
	if (availableDates.length > 0) {
		updateDateSuggestions();
		dateSuggestions.style.display = "block";
	}
}

function hideDateSuggestions() {
	setTimeout(() => {
		dateSuggestions.style.display = "none";
	}, 200);
}

function updateDateSuggestions() {
	if (!availableDates.length) return;

	const filter = dateFilterInput.value.toLowerCase();
	const filteredDates = availableDates.filter((date) =>
		date.toLowerCase().includes(filter)
	);

	dateSuggestions.innerHTML = "";

	// Add individual dates
	filteredDates.forEach((date) => {
		const suggestion = document.createElement("div");
		suggestion.className = "date-suggestion";
		suggestion.textContent = date;
		suggestion.onclick = () => {
			if (filterMode === "include") {
				toggleDateSelection(date);
			} else {
				selectDate(date);
			}
		};
		dateSuggestions.appendChild(suggestion);
	});

	// Add range suggestions
	if (filteredDates.length >= 2 && !filter) {
		const firstDate = filteredDates[0];
		const lastDate = filteredDates[filteredDates.length - 1];

		const rangeSuggestion = document.createElement("div");
		rangeSuggestion.className = "date-suggestion";
		rangeSuggestion.innerHTML = `<strong>Full Range:</strong> ${firstDate} to ${lastDate}`;
		rangeSuggestion.onclick = () => selectDate(`${firstDate}:${lastDate}`);
		dateSuggestions.appendChild(rangeSuggestion);
	}
}

function filterDateSuggestions() {
	updateDateSuggestions();
}

function selectDate(dateValue) {
	dateFilterInput.value = dateValue;
	dateSuggestions.style.display = "none";
}

function applyFilters() {
	const channel = channelSelect.value;
	if (!channel) {
		showNotification("Please select a channel first", "error");
		return;
	}

	// Build the date filter string based on current mode
	const dateFilter = buildDateFilterString();
	currentDateFilter = dateFilter;

	// Show user-friendly feedback
	if (dateFilter) {
		let message = "Applying filter: ";
		if (filterMode === "range") {
			message += `Date range ${dateFilter}`;
		} else if (filterMode === "include") {
			const dateCount = selectedDates.size;
			message += `${dateCount} selected date${dateCount !== 1 ? "s" : ""}`;
		} else if (filterMode === "exclude") {
			const dateCount = excludedDates.size;
			message += `Excluding ${dateCount} date${dateCount !== 1 ? "s" : ""}`;
		}
		showNotification(message, "success");
	}

	// Fetch filtered data for the selected channel. The socket 'channel_data' handler
	// will update the side-menu details once the filtered data is returned.
	requestChannelData(channel, currentDateFilter);
	// Show the side info container; it will be populated when 'channel_data' arrives
	document.getElementById("selectedChannelInfo").style.display = "block";
}

function clearFilters() {
	dateFilterInput.value = "";
	selectedDates.clear();
	excludedDates.clear();
	updateSelectedDatesDisplay();
	currentDateFilter = null;

	if (selectedChannel) {
		requestChannelData(selectedChannel, null);
		showNotification("Filters cleared", "success");
	}
}

// Add missing functions for the new enhanced system
function showDateSuggestions() {
	if (availableDates.length > 0) {
		updateDateSuggestions();
		dateSuggestions.style.display = "block";
	}
}

function hideDateSuggestions() {
	// Delay hiding to allow for clicks
	setTimeout(() => {
		dateSuggestions.style.display = "none";
	}, 150);
}

function filterDateSuggestions() {
	updateDateSuggestions();
}

// User Detail Popup Functionality
let currentUserData = null;
let currentMessagesPage = 1;
let userMessageCache = {};

function openUserDetail(username) {
	if (!selectedChannel) {
		showNotification("Please select a channel first", "error");
		return;
	}

	// Show popup immediately with loading state
	document.getElementById("userDetailOverlay").style.display = "flex";
	document.getElementById("userDetailName").textContent = username;
	document.getElementById("userDetailSubtitle").textContent = "Loading...";
	document.getElementById("userAvatar").textContent = username
		.charAt(0)
		.toUpperCase();

	// Reset to overview tab
	switchUserTab("overview");
	currentMessagesPage = 1;

	// Load user details
	loadUserDetails(username);
}

async function loadUserDetails(username) {
	try {
		const currentFilter = buildDateFilterString();
		const params = new URLSearchParams({
			channel: selectedChannel,
			page: 1,
			limit: 100,
		});

		if (currentFilter) {
			params.append("date_filter", currentFilter);
		}

		const response = await fetch(
			`/api/user/${encodeURIComponent(username)}?${params}`
		);
		const data = await response.json();

		if (data.error) {
			showNotification(data.error, "error");
			closeUserDetail();
			return;
		}

		currentUserData = data;
		populateUserDetail(data);
	} catch (error) {
		console.error("Error loading user details:", error);
		showNotification("Failed to load user details", "error");
		closeUserDetail();
	}
}

function populateUserDetail(data) {
	// Update header
	document.getElementById("userDetailName").textContent = data.username;
	document.getElementById(
		"userDetailSubtitle"
	).textContent = `${data.stats.chat_count} messages in ${selectedChannel}`;

	// Populate overview tab
	populateOverviewTab(data);

	// Populate messages tab
	populateMessagesTab(data);

	// Populate channels tab
	populateChannelsTab(data);

	// Populate activity tab
	populateActivityTab(data);
}

function populateOverviewTab(data) {
	const statsGrid = document.getElementById("userStatsGrid");

	const altBadgeClass =
		data.stats.alt_likelihood < 30
			? "low"
			: data.stats.alt_likelihood < 70
			? "medium"
			: "high";

	statsGrid.innerHTML = `
					<div class="user-stat-card">
						<span class="user-stat-value">${data.stats.chat_count.toLocaleString()}</span>
						<div class="user-stat-label">Total Messages</div>
					</div>
					<div class="user-stat-card">
						<span class="user-stat-value alt-badge ${altBadgeClass}">${data.stats.alt_likelihood.toFixed(
		1
	)}%</span>
						<div class="user-stat-label">Alt Likelihood</div>
					</div>
					<div class="user-stat-card">
						<span class="user-stat-value">${data.channels.length}</span>
						<div class="user-stat-label">Active Channels</div>
					</div>
					<div class="user-stat-card">
						<span class="user-stat-value">${data.activity_timeline.length}</span>
						<div class="user-stat-label">Active Days</div>
					</div>
				`;

	const similarUsersDiv = document.getElementById("userSimilarUsers");
	if (data.stats.similar_users && data.stats.similar_users.length > 0) {
		similarUsersDiv.innerHTML = data.stats.similar_users.join(", ");
	} else {
		similarUsersDiv.innerHTML = "<em>No similar users detected</em>";
	}
}

function populateMessagesTab(data) {
	const messagesList = document.getElementById("userMessagesList");

	if (data.messages.length === 0) {
		messagesList.innerHTML =
			'<div style="text-align: center; padding: 2rem; color: var(--text-muted);">No messages found</div>';
		return;
	}

	messagesList.innerHTML = data.messages
		.map((msg) => {
			const timestamp = new Date(msg.timestamp);
			const dateStr = msg.log_date;
			const timeStr = timestamp.toLocaleTimeString();

			return `
						<div class="user-message-item">
							<div class="user-message-meta">
								<span class="user-message-date">${dateStr}</span>
								<span class="user-message-time">${timeStr}</span>
							</div>
							<div class="user-message-text">${escapeHtml(msg.message)}</div>
						</div>
					`;
		})
		.join("");

	// Update pagination
	const pagination = document.getElementById("messagesPagination");
	const hasMore = data.pagination.has_more;
	const currentPage = data.pagination.page;

	pagination.innerHTML = `
					<button class="pagination-btn" ${currentPage <= 1 ? "disabled" : ""} 
							onclick="loadUserMessagesPage(${currentPage - 1})">
						<i class="fas fa-chevron-left"></i> Previous
					</button>
					<span>Page ${currentPage}</span>
					<button class="pagination-btn" ${!hasMore ? "disabled" : ""} 
							onclick="loadUserMessagesPage(${currentPage + 1})">
						Next <i class="fas fa-chevron-right"></i>
					</button>
				`;
}

function populateChannelsTab(data) {
	const channelsList = document.getElementById("userChannelsList");

	if (data.channels.length === 0) {
		channelsList.innerHTML =
			'<div style="text-align: center; padding: 2rem; color: var(--text-muted);">No channel activity found</div>';
		return;
	}

	channelsList.innerHTML = data.channels
		.map(
			(channel) => `
					<div class="user-channel-item">
						<div>
							<div class="user-channel-name">#${channel.channel}</div>
							<div class="user-channel-stats">
								${channel.message_count.toLocaleString()} messages • 
								${channel.first_message_date} to ${channel.last_message_date}
							</div>
						</div>
						<div style="text-align: right;">
							<div style="font-weight: 600; color: var(--text-primary);">
								${channel.message_count.toLocaleString()}
							</div>
							<div style="font-size: 0.75rem; color: var(--text-muted);">messages</div>
						</div>
					</div>
				`
		)
		.join("");
}

function populateActivityTab(data) {
	const timeline = document.getElementById("userActivityTimeline");

	if (data.activity_timeline.length === 0) {
		timeline.innerHTML =
			'<div style="text-align: center; padding: 2rem; color: var(--text-muted);">No activity data found</div>';
		return;
	}

	timeline.innerHTML = data.activity_timeline
		.map((day) => {
			const firstTime = new Date(day.first_message_time).toLocaleTimeString();
			const lastTime = new Date(day.last_message_time).toLocaleTimeString();

			return `
						<div class="user-activity-day">
							<div class="user-activity-date">${day.date}</div>
							<div class="user-activity-stats">
								<span><i class="fas fa-comments"></i> ${day.message_count}</span>
								<span><i class="fas fa-clock"></i> ${firstTime} - ${lastTime}</span>
								<span><i class="fas fa-chart-line"></i> ${day.active_hours}h active</span>
							</div>
						</div>
					`;
		})
		.join("");
}

async function loadUserMessagesPage(page) {
	if (!currentUserData) return;

	currentMessagesPage = page;
	const messagesList = document.getElementById("userMessagesList");
	messagesList.innerHTML =
		'<div style="text-align: center; padding: 2rem;"><div class="loading-spinner-small"></div> Loading messages...</div>';

	try {
		const currentFilter = buildDateFilterString();
		const params = new URLSearchParams({
			channel: selectedChannel,
			page: page,
			limit: 100,
		});

		if (currentFilter) {
			params.append("date_filter", currentFilter);
		}

		const response = await fetch(
			`/api/user/${encodeURIComponent(currentUserData.username)}?${params}`
		);
		const data = await response.json();

		if (data.error) {
			showNotification(data.error, "error");
			return;
		}

		// Update only the messages part
		currentUserData.messages = data.messages;
		currentUserData.pagination = data.pagination;
		populateMessagesTab(currentUserData);
	} catch (error) {
		console.error("Error loading messages page:", error);
		showNotification("Failed to load messages", "error");
	}
}

function switchUserTab(tabName) {
	// Remove active class from all tabs and contents
	document
		.querySelectorAll(".user-detail-tab")
		.forEach((tab) => tab.classList.remove("active"));
	document
		.querySelectorAll(".user-detail-tab-content")
		.forEach((content) => content.classList.remove("active"));

	// Add active class to selected tab and content
	event.target.classList.add("active");
	document.getElementById(tabName + "Tab").classList.add("active");
}

function closeUserDetail() {
	document.getElementById("userDetailOverlay").style.display = "none";
	currentUserData = null;
	currentMessagesPage = 1;
}

function filterUserMessages() {
	const searchTerm = document
		.getElementById("messageSearch")
		.value.toLowerCase();
	const messageItems = document.querySelectorAll(".user-message-item");

	messageItems.forEach((item) => {
		const messageText = item
			.querySelector(".user-message-text")
			.textContent.toLowerCase();
		if (messageText.includes(searchTerm)) {
			item.style.display = "block";
		} else {
			item.style.display = "none";
		}
	});
}

function clearMessageFilter() {
	document.getElementById("messageSearch").value = "";
	filterUserMessages();
}

function escapeHtml(text) {
	const div = document.createElement("div");
	div.textContent = text;
	return div.innerHTML;
}

// Close popup when clicking outside
document
	.getElementById("userDetailOverlay")
	.addEventListener("click", function (e) {
		if (e.target === this) {
			closeUserDetail();
		}
	});

// Close popup with Escape key
document.addEventListener("keydown", function (e) {
	if (
		e.key === "Escape" &&
		document.getElementById("userDetailOverlay").style.display === "flex"
	) {
		closeUserDetail();
	}
});

function filterTable() {
	const searchTerm = document.getElementById("searchInput").value.toLowerCase();
	const table = document.getElementById("dataTable");
	const rows = table.getElementsByTagName("tr");

	for (let i = 1; i < rows.length; i++) {
		const username = rows[i].cells[0].textContent.toLowerCase();
		const visible = username.includes(searchTerm);
		rows[i].style.display = visible ? "" : "none";
	}
}

function sortTable() {
	const sortBy = document.getElementById("sortSelect").value;
	if (!currentChannelData) return;

	let sortedData = [...currentChannelData.user_stats];

	sortedData.sort((a, b) => {
		if (sortBy === "username") {
			return a.username.localeCompare(b.username);
		} else {
			return b[sortBy] - a[sortBy];
		}
	});

	currentChannelData.user_stats = sortedData;
	displayChannelData(currentChannelData);
}

function sortByColumn(column) {
	const isAscending = sortDirection[column] || false;
	sortDirection[column] = !isAscending;

	if (!currentChannelData) return;

	let sortedData = [...currentChannelData.user_stats];

	sortedData.sort((a, b) => {
		let aVal = a[column];
		let bVal = b[column];

		if (column === "username") {
			return isAscending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
		} else {
			return isAscending ? aVal - bVal : bVal - aVal;
		}
	});

	currentChannelData.user_stats = sortedData;
	displayChannelData(currentChannelData);
}

function updateLastUpdateTime() {
	const now = new Date();
	lastUpdateElement.textContent = `Last updated: ${now.toLocaleTimeString()}`;
}

function showNotification(message, type = "success") {
	const notification = document.createElement("div");
	notification.className = `notification ${type}`;
	notification.textContent = message;
	document.body.appendChild(notification);

	setTimeout(() => notification.classList.add("show"), 100);
	setTimeout(() => {
		notification.classList.remove("show");
		setTimeout(() => notification.remove(), 300);
	}, 3000);
}
// User Detail Analysis Functions
async function loadUserAnalysis(username) {
	try {
		const channel = selectedChannel;
		const dateFilter = buildDateFilterString();

		// Fetch temporal analysis
		const temporalResponse = await fetch(
			`/api/user/${username}/temporal_analysis?channel=${channel}&date_filter=${
				dateFilter || ""
			}`
		);
		const temporalData = await temporalResponse.json();

		// Fetch behavioral insights
		const behavioralResponse = await fetch(
			`/api/user/${username}/behavioral_insights?channel=${channel}&date_filter=${
				dateFilter || ""
			}`
		);
		const behavioralData = await behavioralResponse.json();

		// Populate temporal analysis
		populateTemporalAnalysis(temporalData);

		// Populate behavioral insights
		populateBehavioralInsights(behavioralData);

		// Create hourly activity chart
		createHourlyActivityChart(temporalData.hourly_distribution || {});
	} catch (error) {
		console.error("Error loading user analysis:", error);
		showNotification("Error loading analysis data", "error");
	}
}

function populateTemporalAnalysis(data) {
	const grid = document.getElementById("temporalAnalysisGrid");
	grid.innerHTML = "";

	const stats = [
		{
			value: data.avg_messages_per_day || 0,
			label: "Messages Per Day",
			sublabel: "Average daily activity",
		},
		{
			value: data.avg_messages_per_hour || 0,
			label: "Messages Per Hour",
			sublabel: "When active",
		},
		{
			value: data.avg_session_length_hours || 0,
			label: "Session Length",
			sublabel: "Hours per session",
		},
		{
			value: data.peak_activity_hour || "12",
			label: "Peak Hour",
			sublabel: `${data.peak_hour_messages || 0} messages`,
		},
		{
			value: data.active_days || 0,
			label: "Active Days",
			sublabel: "Total days with activity",
		},
		{
			value: (data.consistency_score * 100).toFixed(1) + "%",
			label: "Consistency Score",
			sublabel: "How regular is activity",
		},
	];

	stats.forEach((stat) => {
		const card = document.createElement("div");
		card.className = "analysis-stat-card";
		card.innerHTML = `
						<span class="analysis-stat-value">${stat.value}</span>
						<div class="analysis-stat-label">${stat.label}</div>
						<div class="analysis-stat-sublabel">${stat.sublabel}</div>
					`;
		grid.appendChild(card);
	});
}

function populateBehavioralInsights(data) {
	const container = document.getElementById("behavioralInsights");
	container.innerHTML = "";

	const insights = [
		{
			icon: "fas fa-clock",
			title: "Message Frequency",
			description: `${
				data.message_frequency?.per_minute?.toFixed(3) || 0
			} messages per minute, ${
				data.message_frequency?.per_hour?.toFixed(1) || 0
			} per hour`,
		},
		{
			icon: "fas fa-chart-bar",
			title: "Engagement Level",
			description: `${data.engagement_level || "Unknown"} engagement with ${
				data.burst_messaging ? "burst" : "steady"
			} messaging pattern`,
		},
		{
			icon: "fas fa-edit",
			title: "Writing Style",
			description: `Avg ${
				data.writing_style?.avg_message_length?.toFixed(1) || 0
			} chars per message, ${
				(data.writing_style?.question_frequency * 100)?.toFixed(1) || 0
			}% questions`,
		},
		{
			icon: "fas fa-chart-line",
			title: "Activity Consistency",
			description: `${
				(data.activity_consistency * 100)?.toFixed(1) || 0
			}% consistency score across active days`,
		},
	];

	// Add activity patterns if available
	if (data.activity_patterns && data.activity_patterns.length > 0) {
		data.activity_patterns.forEach((pattern) => {
			insights.push({
				icon: "fas fa-analytics",
				title: pattern.pattern,
				description: pattern.description,
			});
		});
	}

	insights.forEach((insight) => {
		const item = document.createElement("div");
		item.className = "behavioral-insight-item";
		item.innerHTML = `
						<div class="behavioral-insight-icon">
							<i class="${insight.icon}"></i>
						</div>
						<div class="behavioral-insight-content">
							<div class="behavioral-insight-title">${insight.title}</div>
							<div class="behavioral-insight-description">${insight.description}</div>
						</div>
					`;
		container.appendChild(item);
	});
}

function createHourlyActivityChart(hourlyData) {
	const canvas = document.getElementById("hourlyActivityChart");
	const ctx = canvas.getContext("2d");

	// Destroy existing chart if it exists
	if (window.hourlyChart) {
		window.hourlyChart.destroy();
	}

	// Prepare data for 24-hour chart
	const hours = Array.from({ length: 24 }, (_, i) =>
		i.toString().padStart(2, "0")
	);
	const data = hours.map((hour) => hourlyData[hour] || 0);

	window.hourlyChart = new Chart(ctx, {
		type: "bar",
		data: {
			labels: hours.map((h) => h + ":00"),
			datasets: [
				{
					label: "Messages",
					data: data,
					backgroundColor: "rgba(102, 126, 234, 0.6)",
					borderColor: "rgba(102, 126, 234, 1)",
					borderWidth: 1,
				},
			],
		},
		options: {
			responsive: true,
			maintainAspectRatio: false,
			plugins: {
				legend: {
					display: false,
				},
			},
			scales: {
				y: {
					beginAtZero: true,
					ticks: {
						color: "#cccccc",
					},
					grid: {
						color: "#444444",
					},
				},
				x: {
					ticks: {
						color: "#cccccc",
						maxRotation: 45,
					},
					grid: {
						color: "#444444",
					},
				},
			},
		},
	});
}

function populateDetailedSimilarUsers(similarUsers) {
	const container = document.getElementById("detailedSimilarUsers");
	container.innerHTML = "";

	if (!similarUsers || similarUsers.length === 0) {
		container.innerHTML =
			'<p style="color: var(--text-muted); text-align: center; padding: 2rem;">No similar users found</p>';
		return;
	}

	similarUsers.forEach((user) => {
		const item = document.createElement("div");
		item.className = "similar-user-item";
		item.onclick = () => openUserDetail(user.username);

		const avatar = user.username.charAt(0).toUpperCase();
		const similarity =
			typeof user.similarity === "number"
				? `${(user.similarity * 100).toFixed(1)}% similar`
				: user.similarity || "Similar user";

		item.innerHTML = `
						<div class="similar-user-info">
							<div class="similar-user-avatar">${avatar}</div>
							<div class="similar-user-details">
								<div class="similar-user-name">${user.username}</div>
								<div class="similar-user-similarity">${similarity}</div>
							</div>
						</div>
						<div class="similar-user-actions">
							<button class="similar-user-btn" onclick="event.stopPropagation(); openUserDetail('${user.username}')">
								View Details
							</button>
						</div>
					`;
		container.appendChild(item);
	});
}

// Sidebar toggle functionality
function toggleSidebar() {
	const sidebar = document.getElementById("sidebar");
	const mainContent = document.getElementById("mainContent");
	const toggleBtn = document.getElementById("sidebarToggle");

	sidebar.classList.toggle("collapsed");
	mainContent.classList.toggle("expanded");

	if (sidebar.classList.contains("collapsed")) {
		toggleBtn.classList.remove("expanded");
		toggleBtn.classList.add("collapsed");
		toggleBtn.innerHTML = '<i class="fas fa-bars"></i>';
	} else {
		toggleBtn.classList.remove("collapsed");
		toggleBtn.classList.add("expanded");
		toggleBtn.innerHTML = '<i class="fas fa-times"></i>';
	}
}

// Handle responsive behavior
function handleResize() {
	const sidebar = document.getElementById("sidebar");
	const mainContent = document.getElementById("mainContent");
	const toggleBtn = document.getElementById("sidebarToggle");

	if (window.innerWidth <= 768) {
		// Mobile behavior
		sidebar.classList.add("collapsed");
		mainContent.classList.add("expanded");
		toggleBtn.classList.remove("expanded");
		toggleBtn.classList.add("collapsed");
	} else {
		// Desktop behavior - restore sidebar if it was auto-collapsed
		if (!sidebar.dataset.userCollapsed) {
			sidebar.classList.remove("collapsed");
			mainContent.classList.remove("expanded");
			toggleBtn.classList.remove("collapsed");
			toggleBtn.classList.add("expanded");
		}
	}
}

// Track user manual collapse/expand
const originalToggleSidebar = toggleSidebar;
toggleSidebar = function () {
	const sidebar = document.getElementById("sidebar");
	sidebar.dataset.userCollapsed = sidebar.classList.contains("collapsed")
		? "false"
		: "true";
	originalToggleSidebar();
};

// Add resize listener
window.addEventListener("resize", handleResize);

// Initialize responsive behavior
document.addEventListener("DOMContentLoaded", handleResize);

// Handle processing status updates
socket.on("processing_status", function (data) {
	console.log("Processing status:", data);

	const statusBar = document.querySelector(".status-bar");
	const lastUpdate = document.getElementById("lastUpdate");

	if (data.status === "processing") {
		lastUpdate.innerHTML = `
						<i class="fas fa-spinner fa-spin" style="color: var(--warning-color);"></i>
						Processing: ${data.message} (${data.progress || ""})
					`;
	} else if (data.status === "complete") {
		lastUpdate.innerHTML = `
						<i class="fas fa-check-circle" style="color: var(--accent-color);"></i>
						${data.message}
					`;
		setTimeout(() => {
			updateLastUpdateTime();
		}, 2000);
	} else if (data.status === "error") {
		lastUpdate.innerHTML = `
						<i class="fas fa-exclamation-triangle" style="color: var(--danger-color);"></i>
						Error: ${data.message}
					`;
	}
});

// Enhanced data update handler
socket.on("data_update", function (data) {
	console.log("Data update received:", data);

	if (data.channels && data.channels.length > 0) {
		// Update channels dropdown if needed
		populateChannels(data.channels);

		// If current channel is being viewed, refresh its data
		const currentChannel = document.getElementById("channelSelect").value;
		if (currentChannel) {
			// Refresh current view with new data
			onChannelChange();
		}
	}
});

// Show initial loading state for new installations
function showInitialLoadingState() {
	const tableContainer = document.querySelector(".table-container");
	const loadingSpinner = document.getElementById("loadingSpinner");
	const dataTable = document.getElementById("dataTable");

	if (loadingSpinner && dataTable) {
		loadingSpinner.style.display = "flex";
		dataTable.style.display = "none";

		const loadingText = loadingSpinner.querySelector("p");
		if (loadingText) {
			loadingText.textContent = "Processing initial data in background...";
		}
	}
}

// Check if we need to show initial loading
socket.on("connect", function () {
	// Check if we have any existing data
	fetch("/api/channels")
		.then((response) => response.json())
		.then((data) => {
			if (!data.channels || data.channels.length === 0) {
				showInitialLoadingState();
				document.getElementById("lastUpdate").innerHTML = `
								<i class="fas fa-clock" style="color: var(--warning-color);"></i>
								Processing initial data - this may take a few moments...
							`;
			}
		})
		.catch(() => {
			// If we can't fetch channels, assume we're still loading
			showInitialLoadingState();
		});
});
// Add this function to prevent ReferenceError and populate channel dropdown
function populateChannels(channels) {
	console.log("channels", channels);

	const channelSelect = document.getElementById("channelSelect");
	if (!channelSelect) return;
	channelSelect.innerHTML = '<option value="">📺 Choose a channel...</option>';
	channels.forEach((channel) => {
		// Support both string and object
		const channelName = typeof channel === "string" ? channel : channel.channel;
		const option = document.createElement("option");
		option.value = channelName;
		option.textContent = `📺 ${channelName}`;
		channelSelect.appendChild(option);
	});
}
// ...existing JS from dashboard.html will be moved here. This includes WebSocket setup, DOM manipulation, filtering, sorting, user detail popup logic, and all other dashboard functions...
