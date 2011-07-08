namespace CodeCamp.Domain
{
    using System;
    using Magnum.Data;
    using Magnum.Extensions;
    using MassTransit;
    using MassTransit.Saga;
    using MassTransit.Services.Timeout.Messages;
    using Messages;
    using PostalService.Messages;

    public class RegisterUserSaga :
        InitiatedBy<RegisterUser>,
        Orchestrates<UserVerificationEmailSent>,
        Orchestrates<UserVerifiedEmail>,
        Orchestrates<EmailSent>,
        ISaga
    {
        private readonly Guid _correlationId;
        private DateTime? _lastEmailSent;
        private User _user;

        public RegisterUserSaga(Guid correlationId)
        {
            _correlationId = correlationId;
        }

        protected RegisterUserSaga()
        {
        }

        public User User
        {
            get { return _user; }
        }

        #region InitiatedBy<RegisterUser> Members

        public void Consume(RegisterUser message)
        {
            _user = new User(message.Name, message.Username, message.Password, message.Email);

            string body = string.Format("Please verify email http://localhost/ConfirmEmail/?registrationId={0}",
                                        _correlationId);
            Bus.Publish(new SendEmail(CorrelationId, _user.Email, "dru@drusellers.com", "verify email", body));
        }

        #endregion

        #region ISaga Members

        public Guid CorrelationId
        {
            get { return _correlationId; }
        }

        public IServiceBus Bus { get; set; }

        public IObjectBuilder ServiceLocator { get; set; }

        #endregion

        #region Orchestrates<EmailSent> Members

        public void Consume(EmailSent message)
        {
            _lastEmailSent = message.SentAt;

            Bus.Publish(new UserVerificationEmailSent(_correlationId));
        }

        #endregion

        #region Orchestrates<UserVerificationEmailSent> Members

        public void Consume(UserVerificationEmailSent message)
        {
            _user.SetEmailPending();

            Bus.Publish(new ScheduleTimeout(CorrelationId, 24.Hours().FromNow()));
        }

        #endregion

        #region Orchestrates<UserVerifiedEmail> Members

        public void Consume(UserVerifiedEmail message)
        {
            _user.ConfirmEmail();

            string body = string.Format("Thank you. You are now registered");

            // use a new guid because we don't want any more messages to this saga about e-mails
            Bus.Publish(new SendEmail(Guid.Empty, _user.Email, "dru@drusellers.com", "Register Successful", body));
        }

        #endregion
    }
}