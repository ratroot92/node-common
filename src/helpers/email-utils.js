const nodemailer = require('nodemailer');

// SMTP_HOST="mail.pec.org.pk"
// SMTP_PORT=25
// SMTP_SECURE=false
// SMTP_AUTH_USER="no-reply@pec.org.pk"
// SMTP_AUTH_PASS= "Noreply@12#$43@!"

const emailUtils = {
  host: process.env.SMTP_HOST || 'mail.pec.org.pk',
  port: Number(process.env.SMTP_PORT) || 25,
  // secure: Boolean(process.env.SMTP_SECURE) || false,
  secure: true,

  auth: {
    user: process.env.SMTP_AUTH_USER || 'no-reply@pec.org.pk',
    pass: process.env.SMTP_AUTH_PASS || 'Noreply@12#$43@!',
  },
  reciever: 'maliksblr92@gmail.com',
  transport: function () {
    return nodemailer.createTransport({
      host: this.host,
      port: this.port,
      secure: this.secure,
      auth: this.auth,
    });
  },

  send: async function (options = {}) {
    try {
      const mailTransporter = this.transport();
      let mailData = {
        from: this.host,
        to: this.reciever,
        subject: options.subject || 'Email Subject',
        text: options.text || 'Otp',
        //   attachments: [{ filename: 'epe_rollno_slip.pdf', path: sysPath, contentType: 'application/pdf' }],
        html: options.html || `<p> This email is originated from www.evergreen.com<p><p>Your Otp is <b>${options.otp} </b> </p>`,
      };
      console.log(this);
      console.log(mailData);

      return mailTransporter.sendMail(mailData);
    } catch (err) {
      console.log('============================');
      console.log(err.stack);
      console.log('============================');

      throw new Error(err.message);
    }
  },
};

module.exports = emailUtils;
