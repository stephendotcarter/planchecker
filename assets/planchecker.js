function savePlan(planId){
    // Show spinner
    $('#saveSpinner').removeClass('hidden');
    $.ajax({
        method: "POST",
        url: "/plan/" + planId,
        dataType: "json",
        data: { action: "save", planId: planId }
    }).done(function( res ) {
        if (res.status == "success") {
            $('#planRef').removeClass('hidden');
            $('#bookmarkMsg').removeClass('hidden');
            $('#planSave').addClass('hidden');
        } else if (res.status == "failure") {
            alert("Oops...");
        }

        // Remove spinner
        $('#saveSpinner').addClass('hidden');
    });
}

$(function () {
    // Initialize tooltips
    $('[data-toggle="tooltip"]').tooltip()
});
