# Plan: Make "Show Health" only show selected disks in web app

## Context
The "Show Health" button in the web UI currently shows health for ALL disks, even when specific disks are selected. Looking at the `runHealthEnrichment` function in the frontend:

```javascript
const selectedPaths = fromSlotAction ? selectedPresent.map(s => s.device.path) : [];
```

When `fromSlotAction` is false (Show Health button clicked from slots view), it passes an empty array, which causes the backend to scan all devices instead of just the selected ones.

## Implementation Approach

### File to Modify
- `/root/diskmanager/web/static/index.html` - `runHealthEnrichment` function (around line 4106)

### Changes Required

Change line 4106 from:
```javascript
const selectedPaths = fromSlotAction ? selectedPresent.map(s => s.device.path) : [];
```

To:
```javascript
const selectedPaths = selectedPresent.map(s => s.device.path);
```

This ensures that whether the user clicks "Show Health" from the slots view or from the slot detail panel, it always scans only the currently selected disks.

## Verification

1. Open the web app and select a few disks in the slots view
2. Click "Show Health" - should only show health for the selected disks
3. Click on a single disk's detail panel and click "Health" - should show health for that disk
4. Select all disks and click "Show Health" - should show health for all disks
5. With no disks selected, click "Show Health" - should not scan any disks (empty selection)
