(function () {
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach((alert) => {
        setTimeout(() => {
            alert.classList.add('fade');
            alert.addEventListener('transitionend', () => alert.remove(), { once: true });
        }, 4000);
    });
})();
