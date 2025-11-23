#!/bin/bash
# Setup cron job for hourly data generation

echo "======================================================================"
echo "⏰ Setting up Hourly Data Generation Cron Job"
echo "======================================================================"

CRON_LINE="0 * * * * cd /Users/amohiuddeen/Github/dq-framework-poc && /Users/amohiuddeen/Github/dq-framework-poc/run_data_injection.sh >> /Users/amohiuddeen/Github/dq-framework-poc/logs/data_injection.log 2>&1"

# Create logs directory
mkdir -p /Users/amohiuddeen/Github/dq-framework-poc/logs

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "run_data_injection.sh"; then
    echo "⚠️  Cron job already exists!"
    echo ""
    echo "Current cron jobs:"
    crontab -l | grep data_injection
else
    echo "📝 Adding cron job..."
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "✅ Cron job added successfully!"
fi

echo ""
echo "======================================================================"
echo "Cron Schedule: Every hour at minute 0"
echo "Script: /Users/amohiuddeen/Github/dq-framework-poc/run_data_injection.sh"
echo "Log file: /Users/amohiuddeen/Github/dq-framework-poc/logs/data_injection.log"
echo ""
echo "To view current cron jobs:"
echo "  crontab -l"
echo ""
echo "To remove this cron job:"
echo "  crontab -l | grep -v 'run_data_injection.sh' | crontab -"
echo ""
echo "To view logs:"
echo "  tail -f /Users/amohiuddeen/Github/dq-framework-poc/logs/data_injection.log"
echo "======================================================================"

