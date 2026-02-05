# FlowForge Production Deployment Guide

## Quick Deploy to Railway (5 minutes)

### 1. Prerequisites
- GitHub account
- Railway account (free - sign up at railway.app)

### 2. Push to GitHub
```bash
cd C:/Users/kevin/projects/universal-integrator
git init
git add .
git commit -m "Initial commit - FlowForge v1.0"
gh repo create flowforge --public --source=. --push
```

### 3. Deploy on Railway

1. Go to https://railway.app
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Choose your `flowforge` repository
5. Railway will automatically:
   - Detect the Dockerfile
   - Provision PostgreSQL database
   - Provision Redis cache
   - Set DATABASE_URL and REDIS_URL
   - Deploy your app

### 4. Add Environment Variables

In Railway dashboard, add these:
```
SECRET_KEY=<generate-random-32-char-string>
API_KEY_SALT=<generate-random-32-char-string>
ENVIRONMENT=production
ALLOWED_ORIGINS=https://your-domain.com
```

### 5. Get Your URL

Railway gives you a URL like: `flowforge-production.up.railway.app`

**Done!** Your app is live at that URL.

---

## What Railway Provides Automatically

✅ **PostgreSQL Database** - Fully managed, auto-backups
✅ **Redis Cache** - For task queues and caching
✅ **Auto-scaling** - Handles traffic spikes
✅ **SSL Certificate** - HTTPS enabled
✅ **Environment Variables** - Secure secret management
✅ **Monitoring** - Built-in metrics dashboard
✅ **Logs** - Real-time application logs
✅ **Rollback** - One-click rollback to previous versions

---

## Database Migrations

Railway runs migrations automatically on deploy (via Dockerfile CMD).

To run manual migrations:
```bash
railway run python -c "from src.db.database import init_db; init_db()"
```

---

## Cost Breakdown

### Railway Pricing
- **Hobby Plan**: $5/month (enough for 500 GB outbound data, $0.000231/min compute)
- **Pro Plan**: $20/month (for production businesses)
- **PostgreSQL**: Included in plan
- **Redis**: Included in plan

### Estimated Monthly Cost (Hobby)
- App hosting: ~$5-10/month (light usage)
- Database: Included
- Total: **~$10/month** for a production-ready business app

---

## Business Features Included

✅ Multi-tenancy (user/organization isolation)
✅ API authentication (API keys)
✅ Rate limiting (prevent abuse)
✅ Encrypted credential storage
✅ Audit logging (all executions tracked)
✅ Webhook security (HMAC verification)
✅ Health checks & monitoring
✅ Error tracking (Sentry integration ready)

---

## Scaling for Growth

Railway auto-scales, but for high-traffic:

1. **Upgrade to Pro**: $20/month, more resources
2. **Add Redis workers**: Scale Celery workers independently
3. **Database replicas**: Add read replicas
4. **CDN**: Add Cloudflare for static assets

---

## Custom Domain

1. In Railway dashboard, go to Settings
2. Add custom domain (e.g., `app.yourcompany.com`)
3. Update DNS records as shown
4. SSL certificate auto-provisioned

---

## Monitoring & Alerts

Railway provides:
- CPU/Memory usage graphs
- Request latency
- Error rates
- Custom alerts

For advanced monitoring, add Sentry:
```python
# Add to .env
SENTRY_DSN=your-sentry-dsn
```

---

## Security Checklist

✅ Use strong SECRET_KEY (32+ random characters)
✅ Set ALLOWED_ORIGINS to your domain only
✅ Enable rate limiting (default: 60 req/min)
✅ Use API keys for authentication
✅ Rotate credentials regularly
✅ Enable database backups (Railway auto-backs up daily)
✅ Monitor error logs

---

## Support & Troubleshooting

### View Logs
```bash
railway logs
```

### SSH into container
```bash
railway shell
```

### Check database
```bash
railway connect postgres
```

---

## Next Steps

1. **Add team members** - Invite employees to your org
2. **Set up monitoring** - Add Sentry for error tracking
3. **Configure webhooks** - Set up external service integrations
4. **API documentation** - Share `/docs` with your team
5. **Backup strategy** - Export workflows weekly

---

**Need help?** Open an issue on GitHub or email support@yourcompany.com
